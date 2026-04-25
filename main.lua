-- Kobo/KOReader integration: kepub browsing, reading progress sync,
-- Nickel sync handoff, collection import, and sync server configuration.

local Device = require("device")
if not Device:isKobo() then
    return { disabled = true }
end

local ConfirmBox           = require("ui/widget/confirmbox")
local DocSettings          = require("docsettings")
local DocumentRegistry     = require("document/documentregistry")
local Event                = require("ui/event")
local FileChooser          = require("ui/widget/filechooser")
local InfoMessage          = require("ui/widget/infomessage")
local InputDialog          = require("ui/widget/inputdialog")
local ReadCollection       = require("readcollection")
local ReadHistory          = require("readhistory")
local SQ3                  = require("lua-ljsqlite3/init")
local Trapper              = require("ui/trapper")
local UIManager            = require("ui/uimanager")
local WidgetContainer      = require("ui/widget/container/widgetcontainer")
local ffiutil              = require("ffi/util")
local lfs                  = require("libs/libkoreader-lfs")
local filemanagerutil      = require("apps/filemanager/filemanagerutil")
local util                 = require("util")
local _                    = require("gettext")
local T                    = ffiutil.template

local getCachedKoboTitle

-- Kepubs in Kobo's store folder are extensionless files.
local function is_kobo_kepub(path)
    if not path then return false end
    if not path:find("/%.kobo/kepub/") then return false end
    local basename = path:match("([^/]+)$") or ""
    return not basename:find("%.")
end

-- Let KOReader browse the Kobo kepub folder.
do
    local new_exclude = {}
    for _, p in ipairs(FileChooser.exclude_dirs) do
        if p ~= "^kepub$" then new_exclude[#new_exclude + 1] = p end
    end
    FileChooser.exclude_dirs = new_exclude
end

-- Route extensionless kepubs through the EPUB provider.
local _orig_isSupported = DocumentRegistry.isSupported
function DocumentRegistry:isSupported(file)
    if is_kobo_kepub(file) then return true end
    return _orig_isSupported(self, file)
end

local _orig_getProvider = DocumentRegistry.getProvider
function DocumentRegistry:getProvider(file, ...)
    if is_kobo_kepub(file) then
        return _orig_getProvider(self, file .. ".epub", ...)
    end
    return _orig_getProvider(self, file, ...)
end

local _orig_getProviders = DocumentRegistry.getProviders
function DocumentRegistry:getProviders(file, ...)
    if is_kobo_kepub(file) then
        return _orig_getProviders(self, file .. ".epub", ...)
    end
    return _orig_getProviders(self, file, ...)
end

local _orig_openDocument = DocumentRegistry.openDocument
function DocumentRegistry:openDocument(file, provider, ...)
    if is_kobo_kepub(file) and not provider then
        provider = _orig_getProvider(self, file .. ".epub")
    end
    return _orig_openDocument(self, file, provider, ...)
end

local _orig_getFileNameSuffix = util.getFileNameSuffix
function util.getFileNameSuffix(file)
    if is_kobo_kepub(file) then return "epub" end
    return _orig_getFileNameSuffix(file)
end

-- Show Kobo titles instead of raw file IDs in file-manager views.
local _orig_splitFileNameType = filemanagerutil.splitFileNameType
function filemanagerutil.splitFileNameType(filepath)
    if is_kobo_kepub(filepath) then
        local filename = filepath:match("([^/]+)$") or filepath
        local title = getCachedKoboTitle and getCachedKoboTitle(filename)
        return title or filename, "epub"
    end
    return _orig_splitFileNameType(filepath)
end

-- Keep each extensionless kepub on its own metadata filename.
local _orig_getSidecarFilename = DocSettings.getSidecarFilename
function DocSettings.getSidecarFilename(doc_path)
    if is_kobo_kepub(doc_path) then
        local basename = doc_path:match("([^/]+)$") or doc_path
        return "metadata." .. basename .. ".lua"
    end
    return _orig_getSidecarFilename(doc_path)
end

-- Force sidecar dirs to be per-book instead of collapsing on ".kobo".
local _orig_getSidecarDir = DocSettings.getSidecarDir
function DocSettings:getSidecarDir(doc_path, force_location)
    if is_kobo_kepub(doc_path) then
        return _orig_getSidecarDir(self, doc_path .. ".epub", force_location)
    end
    return _orig_getSidecarDir(self, doc_path, force_location)
end

-- Uncached kepub covers may not have dimensions yet.
do
    local ok, BookInfoManager = pcall(require, "bookinfomanager")
    if ok and BookInfoManager and BookInfoManager.getCachedCoverSize then
        local _orig_getCachedCoverSize = BookInfoManager.getCachedCoverSize
        function BookInfoManager.getCachedCoverSize(img_w, img_h, max_w, max_h)
            if not img_w or not img_h then return 0, 0, 0 end
            return _orig_getCachedCoverSize(img_w, img_h, max_w, max_h)
        end
    end
end

-- KOReader does not persist this search option.
do
    local ok_fs, FileSearcher = pcall(require, "apps/filemanager/filemanagerfilesearcher")
    if ok_fs and FileSearcher then
        FileSearcher.include_metadata = true
    end
end

local SYNC_DB              = "/mnt/onboard/.kobo/KoboReader.sqlite"
local KEPUB_DIR            = "/mnt/onboard/.kobo/kepub"
local KOBO_CONF            = "/mnt/onboard/.kobo/Kobo/Kobo eReader.conf"
local KOBO_DEFAULT_ENDPOINT = "https://storeapi.kobo.com"
local ONBOARD_DIR          = "/mnt/onboard"
local SHELF_PREFIX         = "◆ "
local SERIES_PREFIX        = "☆ "
local DIRECTION            = { SILENT = 1, PROMPT = 2, NEVER = 3 }

local rss = {
    enabled            = true,
    auto_sync_on_close = true,
    auto_sync_on_open  = true,
    sync_to_kobo       = DIRECTION.SILENT,
    sync_from_kobo     = DIRECTION.PROMPT,
}
local saved_rss = G_reader_settings:readSetting("kobo_rss") or {}
for k, v in pairs(saved_rss) do rss[k] = v end

local function saveRSS()
    G_reader_settings:saveSetting("kobo_rss", rss)
end

local function showInfo(text, timeout, icon)
    local args = { text = text, timeout = timeout }
    if icon then args.icon = icon end
    UIManager:show(InfoMessage:new(args))
end

local function openDB()
    return SQ3.open(SYNC_DB)
end

local function withDB(fn)
    local conn = openDB()
    if not conn then return nil end
    local ok, result = pcall(fn, conn)
    pcall(function() conn:close() end)
    if ok then return result end
    return nil
end

local function queryAll(conn, sql, ...)
    local args = {...}
    local nargs = select("#", ...)
    local ok, stmt = pcall(function() return conn:prepare(sql) end)
    if not ok or not stmt then return nil end
    if nargs > 0 then
        ok = pcall(function() stmt:bind(unpack(args, 1, nargs)) end)
        if not ok then
            pcall(function() stmt:close() end)
            return nil
        end
    end
    local rows
    ok, rows = pcall(function() return stmt:resultset() end)
    pcall(function() stmt:close() end)
    return ok and rows or nil
end

local function queryFirst(conn, sql, ...)
    local rows = queryAll(conn, sql, ...)
    if not rows or not rows[1] or rows[1][1] == nil then return nil end
    local row = {}
    for i, col in ipairs(rows) do
        row[i] = col[1]
    end
    return row
end

local title_cache = {}
getCachedKoboTitle = function(book_id, conn)
    if title_cache[book_id] ~= nil then
        return title_cache[book_id] or nil
    end
    local function readTitle(c)
        local row = queryFirst(c,
            "SELECT Title FROM content WHERE ContentID=? AND ContentType=6 LIMIT 1",
            book_id)
        return row and row[1] ~= "" and row[1] or nil
    end
    local opened = conn and true or false
    local title = conn and readTitle(conn) or withDB(function(c)
        opened = true
        return readTitle(c)
    end)
    if not opened then return nil end
    title_cache[book_id] = title or false
    return title
end

-- Kobo ReadStatus: 0 unopened, 1 reading, 2 finished.
local function koboToKR(n)
    n = tonumber(n) or 0
    if n == 1 then return "reading" end
    if n == 2 then return "complete" end
    return ""
end
local function krToKobo(s)
    if s == "complete" or s == "finished" then return 2 end
    return 1
end

-- Kobo stores UTC timestamps; os.time() interprets tables as local time.
local _utc_offset = os.time() - os.time(os.date("!*t"))
local function parseKoboTS(s)
    if not s or s == "" then return 0 end
    local y, mo, d, h, mi, sec = s:match("(%d+)-(%d+)-(%d+)[T ](%d+):(%d+):(%d+)")
    if not y then return 0 end
    local t = os.time({year=tonumber(y), month=tonumber(mo), day=tonumber(d),
                       hour=tonumber(h), min=tonumber(mi), sec=tonumber(sec)})
    return t and (t - _utc_offset) or 0
end

local function extractBookId(path)
    if not path then return nil end
    return path:match("/kepub/([^/%.]+)$")
end

-- A missing sidecar means KOReader has never opened this book.
local function getKRTimestamp(doc_path)
    for _, entry in ipairs(ReadHistory.hist) do
        if entry.file and entry.file == doc_path then
            if DocSettings:hasSidecarFile(doc_path) then
                return entry.time or 0
            end
            return 0
        end
    end
    return 0
end

-- ContentType 6 holds the book-level progress row.
local function readKoboState(book_id, conn)
    local function readState(c)
        local row = queryFirst(c,
            "SELECT ContentID, DateLastRead, ReadStatus, ___PercentRead " ..
            "FROM content WHERE ContentID=? AND ContentType=6 LIMIT 1",
            book_id)
        if not row then return nil end
        local read_st = tonumber(row[3] or 0)
        local pct     = tonumber(row[4] or 0)
        if read_st == 2 and pct == 0 then pct = 100 end
        return {
            percent_read = pct,
            timestamp    = parseKoboTS(row[2]),
            status       = koboToKR(read_st),
            kobo_status  = read_st,
        }
    end
    return conn and readState(conn) or withDB(readState)
end

-- ContentType 9 rows hold chapter-level offsets and progress.
local function writeKoboState(book_id, pct, ts, kr_status, conn)
    -- Kobo has no equivalent for KOReader's "abandoned" status.
    if kr_status == "abandoned" then return false end
    local function writeState(c)
        local date_str = ts and ts > 0 and os.date("!%Y-%m-%dT%H:%M:%SZ", ts) or ""
        local read_st  = krToKobo(kr_status)
        pct = tonumber(pct) or 0
        local pct_int = math.floor(pct)

        local ch_bm = ""

        local row = queryFirst(c,
            "SELECT ContentID, ___FileOffset, ___FileSize FROM content " ..
            "WHERE BookID=? AND ContentType=9 " ..
            "AND ___FileOffset <= ? ORDER BY ___FileOffset DESC LIMIT 1",
            book_id, pct)
        if row then
            local ch_cid  = row[1]
            local ch_off  = tonumber(row[2]) or 0
            local ch_size = tonumber(row[3]) or 0
            local ch_end  = ch_off + ch_size

            if pct > ch_end then
                local last = queryFirst(c,
                    "SELECT ContentID FROM content WHERE BookID=? " ..
                    "AND ContentType=9 ORDER BY ___FileOffset DESC LIMIT 1",
                    book_id)
                if last then
                    ch_cid = last[1]
                    ch_size = 0
                end
            end

            local ch_pct = ch_size > 0 and ((pct - ch_off) / ch_size) * 100 or 0
            ch_pct = math.max(0, math.min(100, ch_pct))

            local ok_ch, s = pcall(function()
                return c:prepare(
                    "UPDATE content SET ___PercentRead=? WHERE ContentID=? AND ContentType=9")
            end)
            if ok_ch and s then
                pcall(function()
                    s:bind(math.floor(ch_pct), ch_cid)
                    s:step()
                end)
                pcall(function() s:close() end)
            end

            local stripped = ch_cid:match("^[^!]+!(.+)$")
            if stripped and stripped ~= "" then
                ch_bm = stripped:gsub("!", "/") .. "#"
            end
        end

        local ok, s = pcall(function()
            return c:prepare(
                "UPDATE content SET ___PercentRead=?, DateLastRead=?, " ..
                "ReadStatus=?, ChapterIDBookmarked=?, ReadStateSynced='false' " ..
                "WHERE ContentID=? AND ContentType=6")
        end)
        if not ok or not s then return false end
        ok = pcall(function()
            s:bind(pct_int, date_str, read_st, ch_bm, book_id)
            s:step()
        end)
        pcall(function() s:close() end)
        if not ok then return false end

        local after = queryFirst(c,
            "SELECT ___PercentRead, DateLastRead, ReadStatus, ChapterIDBookmarked, ReadStateSynced " ..
            "FROM content WHERE ContentID=? AND ContentType=6 LIMIT 1",
            book_id)
        return after
            and tonumber(after[1] or -1) == pct_int
            and (after[2] or "") == date_str
            and tonumber(after[3] or -1) == read_st
            and (after[4] or "") == ch_bm
            and tostring(after[5]) == "false"
    end
    if conn then return writeState(conn) end
    return withDB(writeState) or false
end

local function decide(dir_setting, is_pull, fn, details, cancel_fn)
    if dir_setting == DIRECTION.NEVER then return false end
    if dir_setting == DIRECTION.SILENT then
        if fn then fn() end
        return true
    end
    local src = is_pull and "Kobo" or "KOReader"
    local dst = is_pull and "KOReader" or "Kobo"
    local text = details and T(
        _("Book: %1\n\n%2: %3% (%4)\n%5: %6% (%7)\n\nSync to %8?"),
        details.title or _("Unknown"),
        src, math.floor(details.src_pct or 0),
            details.src_time and os.date("%Y-%m-%d %H:%M", details.src_time) or _("Never"),
        dst, math.floor(details.dst_pct or 0),
            details.dst_time and os.date("%Y-%m-%d %H:%M", details.dst_time) or _("Never"),
        dst)
        or T(_("Sync reading progress to %1?"), dst)
    if Trapper:isWrapped() then
        local ok3 = Trapper:confirm(text, _("No"), _("Yes"))
        if ok3 and fn then fn() end
        if not ok3 and cancel_fn then cancel_fn() end
        return ok3
    end
    UIManager:show(ConfirmBox:new{
        text            = text,
        ok_text         = _("Yes"),
        cancel_text     = _("No"),
        ok_callback     = function() if fn then fn() end end,
        cancel_callback = function() if cancel_fn then cancel_fn() end end,
    })
    return true
end

local function getBookTitle(book_id, doc_settings, conn)
    local title = doc_settings and doc_settings:readSetting("title")
    if title and title ~= "" then return title end
    return getCachedKoboTitle(book_id, conn) or book_id
end

local function syncOneBook(book_id, doc_settings, doc_path, conn)
    local kobo = readKoboState(book_id, conn)
    if not kobo then return false end
    local kr_pct  = doc_settings:readSetting("percent_finished") or 0
    local kr_ts   = getKRTimestamp(doc_path)
    local summary = doc_settings:readSetting("summary") or {}
    local kr_st   = summary.status or "reading"
    local kobo_done = kobo.status == "complete" or kobo.percent_read >= 100
    local kr_done   = kr_pct >= 1.0 or kr_st == "complete" or kr_st == "finished"
    -- If completion state disagrees, still sync the newer side.
    if kobo_done and kr_done then
        local kobo_complete = kobo.status == "complete"
        local kr_complete   = kr_st == "complete" or kr_st == "finished"
        if kobo_complete == kr_complete then return false end
    end
    local title = getBookTitle(book_id, doc_settings, conn)
    if kobo.timestamp > kr_ts then
        if kobo.kobo_status == 0 and kobo.percent_read == 0 then return false end
        return decide(rss.sync_from_kobo, true, function()
            local p = kobo.percent_read / 100.0
            doc_settings:saveSetting("percent_finished", p)
            doc_settings:saveSetting("last_percent", p)
            local s = doc_settings:readSetting("summary") or {}
            s.status = kobo.status
            if kobo.percent_read >= 100 then s.status = "complete" end
            doc_settings:saveSetting("summary", s)
            doc_settings:flush()
        end, {title=title, src_pct=kobo.percent_read, dst_pct=kr_pct*100,
              src_time=kobo.timestamp, dst_time=kr_ts})
    else
        if kr_ts == 0 then return false end
        return decide(rss.sync_to_kobo, false, function()
            writeKoboState(book_id, kr_pct * 100, kr_ts, kr_st, conn)
        end, {title=title, src_pct=kr_pct*100, dst_pct=kobo.percent_read,
              src_time=kr_ts, dst_time=kobo.timestamp})
    end
end

local function syncAllBooks()
    Trapper:wrap(function()
        local synced = 0
        local total  = 0
        local books  = {}
        for file in lfs.dir(KEPUB_DIR) do
            if file ~= "." and file ~= ".." then
                local path    = KEPUB_DIR .. "/" .. file
                local book_id = extractBookId(path)
                if book_id then table.insert(books, {id=book_id, path=path}) end
            end
        end
        Trapper:setPausedText(_("Abort sync?"), _("Abort"), _("Continue"))
        if not Trapper:info(_("Scanning books\xe2\x80\xa6")) then return end
        local aborted = false
        local db_ok = withDB(function(conn)
            for i, book in ipairs(books) do
                if not Trapper:info(T(_("Syncing: %1 / %2"), i, #books)) then
                    Trapper:clear()
                    aborted = true
                    return
                end
                local ds = DocSettings:open(book.path)
                if ds then
                    total = total + 1
                    if syncOneBook(book.id, ds, book.path, conn) then
                        synced = synced + 1
                    end
                end
            end
            return true
        end)
        if aborted then return end
        if not db_ok then
            Trapper:info(_("Could not open Kobo database."))
            ffiutil.sleep(2)
            Trapper:clear()
            return
        end
        ffiutil.sleep(1)
        Trapper:info(T(_("Synced %1 of %2 books"), synced, total))
        ffiutil.sleep(2)
        Trapper:clear()
    end)
end

-- Show Kobo titles in shortened file-manager paths too.
do
    local _orig_abbreviate = filemanagerutil.abbreviate
    filemanagerutil.abbreviate = function(path)
        if is_kobo_kepub(path) then
            local book_id = extractBookId(path)
            if book_id then
                local title = getCachedKoboTitle(book_id)
                if title then return title end
            end
        end
        return _orig_abbreviate(path)
    end
end

local function readLines(path)
    local f = io.open(path, "r")
    if not f then return nil end
    local lines = {}
    for line in f:lines() do
        table.insert(lines, line)
    end
    f:close()
    return lines
end

local function writeLines(path, lines)
    local f = io.open(path, "w")
    if not f then return false end
    for _, line in ipairs(lines) do
        f:write(line .. "\n")
    end
    f:close()
    return true
end

local function findConfSection(lines, section)
    local in_section = false
    for i, line in ipairs(lines) do
        if line:match("^%[" .. section .. "%]") then
            in_section = true
            local last = i
            for j = i + 1, #lines do
                if lines[j]:match("^%[") then return i, last, j end
                last = j
            end
            return i, last, #lines + 1
        elseif in_section and line:match("^%[") then
            break
        end
    end
end

local function readConfValue(section, key)
    local lines = readLines(KOBO_CONF)
    if not lines then return nil end
    local first, last = findConfSection(lines, section)
    if not first then return nil end

    for i = first + 1, last do
        local value = lines[i]:match("^" .. key .. "=(.+)$")
        if value then return value end
    end
end

local function writeConfValue(section, key, value, insert_if_missing)
    local lines = readLines(KOBO_CONF)
    if not lines then return false, "Could not open " .. KOBO_CONF end
    local first, last, insert_at = findConfSection(lines, section)
    if not first then return false, "[" .. section .. "] section not found in Kobo eReader.conf" end

    for i = first + 1, last do
        if lines[i]:match("^" .. key .. "=") then
            lines[i] = key .. "=" .. value
            return writeLines(KOBO_CONF, lines), "Could not write " .. KOBO_CONF
        end
    end

    if not insert_if_missing then return false, key .. " key not found in conf" end

    table.insert(lines, insert_at, key .. "=" .. value)
    return writeLines(KOBO_CONF, lines), "Could not write " .. KOBO_CONF
end

local function setSyncOnNextBoot()
    return writeConfValue("OneStoreServices", "syncOnNextBoot", "true", true)
end

local function doNickelSync()
    UIManager:show(ConfirmBox:new{
        text = _(
            "KOReader will exit and your Kobo library will sync.\n\n"..
            "Once the sync is complete, reopen KOReader.\n\n"..
            "Continue?"),
        ok_text     = _("Exit & Sync"),
        cancel_text = _("Cancel"),
        ok_callback = function()
            local ok2, err = setSyncOnNextBoot()
            if ok2 then
                showInfo(_("Please wait\xe2\x80\xa6"), nil, "notice-info")
                UIManager:forceRePaint()
                UIManager:scheduleIn(1.5, function()
                    UIManager:quit(0)
                end)
            else
                showInfo(_("Failed to schedule sync:\n") .. tostring(err), 3)
            end
        end,
    })
end

local KoboInt = WidgetContainer:extend{ name = "koboko" }

local function isKepubHomeEnabled()
    return G_reader_settings:isTrue("koboko_kepub_home")
end

local function setKepubHome(enabled)
    G_reader_settings:saveSetting("koboko_kepub_home", enabled)
    if enabled then
        G_reader_settings:saveSetting("home_dir", KEPUB_DIR)
    else
        G_reader_settings:saveSetting("home_dir", ONBOARD_DIR)
    end
end

local function patchKOSyncPull(ui)
    local kosync = ui and ui.kosync
    if not kosync then return end
    if kosync.koboko_patched_get_progress or not kosync.getProgress then return kosync end

    local orig_getProgress = kosync.getProgress
    kosync.getProgress = function(self, ensure_networking, interactive, ...)
        if not interactive and self.koboko_auto_pull_mode == "skip" then
            self.koboko_auto_pull_mode = nil
            return
        end
        if not interactive and self.koboko_auto_pull_mode == "defer" then
            local args = {...}
            UIManager:scheduleIn(1, function()
                self:getProgress(ensure_networking, interactive, unpack(args))
            end)
            return
        end
        return orig_getProgress(self, ensure_networking, interactive, ...)
    end
    kosync.koboko_patched_get_progress = true
    return kosync
end

local function skipNextKOSyncAutoPull(ui)
    local kosync = patchKOSyncPull(ui)
    if kosync then
        kosync.koboko_auto_pull_mode = "skip"
    end
end

local function deferNextKOSyncAutoPull(ui)
    local kosync = patchKOSyncPull(ui)
    if kosync then
        kosync.koboko_auto_pull_mode = "defer"
    end
end

local function allowNextKOSyncAutoPull(ui)
    local kosync = ui and ui.kosync
    if kosync then
        kosync.koboko_auto_pull_mode = nil
    end
end

function KoboInt:init()
    self.ui.menu:registerToMainMenu(self)
    patchKOSyncPull(self.ui)
    -- Keep the kepub home setting sticky across restarts.
    if isKepubHomeEnabled() and self.ui.name == "filemanager" then
        G_reader_settings:saveSetting("home_dir", KEPUB_DIR)
    end
end

function KoboInt:onSaveSettings()
    if not rss.enabled or not rss.auto_sync_on_close then
        self.koboko_close_state = nil
        return
    end
    if not self.ui.document or not self.ui.doc_settings then return end

    local path    = self.ui.document.file
    local book_id = extractBookId(path)
    if not book_id then return end

    local kr_pct = self.ui.doc_settings:readSetting("percent_finished") or 0
    local summary = self.ui.doc_settings:readSetting("summary") or {}
    local kr_st  = summary.status or "reading"
    self.koboko_close_state = {
        book_id = book_id,
        pct     = kr_pct,
        status  = kr_st,
        ts      = os.time(),
    }
end

function KoboInt:onCloseDocument()
    if not rss.enabled or not rss.auto_sync_on_close then return end
    if rss.sync_to_kobo ~= DIRECTION.SILENT then
        self.koboko_close_state = nil
        return
    end

    if self.ui.document and self.ui.doc_settings then
        pcall(function()
            self.ui:handleEvent(Event:new("SaveSettings"))
        end)
    end

    local close_state = self.koboko_close_state
    if not close_state then
        if not self.ui.document or not self.ui.doc_settings then return end

        local path    = self.ui.document.file
        local book_id = extractBookId(path)
        if not book_id then return end

        local kr_pct = self.ui.doc_settings:readSetting("percent_finished") or 0
        local summary = self.ui.doc_settings:readSetting("summary") or {}
        close_state = {
            book_id = book_id,
            pct     = kr_pct,
            status  = summary.status or "reading",
            ts      = os.time(),
        }
    end

    local kr_done = close_state.pct >= 1.0
        or close_state.status == "complete"
        or close_state.status == "finished"
    if kr_done then
        local kobo = readKoboState(close_state.book_id)
        if kobo and kobo.status == "complete" then
            self.koboko_close_state = nil
            return
        end
    end

    writeKoboState(close_state.book_id, close_state.pct * 100, close_state.ts, close_state.status)
    self.koboko_close_state = nil
end

-- Pull Kobo progress on open only when Kobo is clearly ahead.
function KoboInt:onReaderReady()
    if not rss.enabled or not rss.auto_sync_on_open then return end
    if not self.ui.document or not self.ui.doc_settings then return end

    local path    = self.ui.document.file
    local book_id = extractBookId(path)
    if not book_id then return end

    local kobo = readKoboState(book_id)
    if not kobo then return end

    if kobo.kobo_status == 0 and kobo.percent_read == 0 then return end

    local kr_pct = self.ui.doc_settings:readSetting("percent_finished") or 0
    local kr_ts  = getKRTimestamp(path)
    local summary = self.ui.doc_settings:readSetting("summary") or {}
    local kr_st  = summary.status or "reading"

    local kobo_done = kobo.status == "complete" or kobo.percent_read >= 100
    local kr_done   = kr_pct >= 1.0 or kr_st == "complete" or kr_st == "finished"

    local kobo_ahead = kobo.percent_read > (kr_pct * 100) + 0.5
    local newer_state = kobo.timestamp > kr_ts
    if kobo_done and kr_done then
        local kobo_complete = kobo.status == "complete"
        local kr_complete   = kr_st == "complete" or kr_st == "finished"
        if kobo_complete == kr_complete then return end
    elseif not (kobo_ahead or (kobo_done and not kr_done) or newer_state) then
        return
    end

    local title = getBookTitle(book_id, self.ui.doc_settings)
    if rss.sync_from_kobo == DIRECTION.SILENT then
        skipNextKOSyncAutoPull(self.ui)
    elseif rss.sync_from_kobo == DIRECTION.PROMPT then
        deferNextKOSyncAutoPull(self.ui)
    end
    decide(rss.sync_from_kobo, true, function()
        skipNextKOSyncAutoPull(self.ui)
        local p = kobo.percent_read / 100.0
        self.ui.doc_settings:saveSetting("percent_finished", p)
        self.ui.doc_settings:saveSetting("last_percent", p)
        local s = self.ui.doc_settings:readSetting("summary") or {}
        s.status = kobo.status
        if kobo.percent_read >= 100 then s.status = "complete" end
        self.ui.doc_settings:saveSetting("summary", s)
        self.ui.doc_settings:flush()
        self.ui:handleEvent(Event:new("GotoPercent", p * 100))
    end, {
        title     = title,
        src_pct   = kobo.percent_read,
        dst_pct   = kr_pct * 100,
        src_time  = kobo.timestamp,
        dst_time  = kr_ts,
    }, function()
        allowNextKOSyncAutoPull(self.ui)
    end)
end

local function isSyncedCollectionName(coll_name)
    return coll_name:sub(1, #SHELF_PREFIX) == SHELF_PREFIX
        or coll_name:sub(1, #SERIES_PREFIX) == SERIES_PREFIX
end

local function syncKoboCollections()
    local added   = 0
    local removed = 0
    local skipped = 0
    local changed = false
    local desired = {}

    local function addDesired(file_path, coll_name)
        local real_path = ffiutil.realpath(file_path) or file_path
        desired[coll_name] = desired[coll_name] or {}
        desired[coll_name][real_path] = true
        if not ReadCollection.coll[coll_name] then
            ReadCollection:addCollection(coll_name)
            changed = true
        end
        if not ReadCollection:isFileInCollection(file_path, coll_name) then
            ReadCollection:addItem(file_path, coll_name)
            added = added + 1
            changed = true
        else
            skipped = skipped + 1
        end
    end

    local ok = withDB(function(conn)
        local shelves_res = queryAll(conn,
            "SELECT Name FROM Shelf WHERE _IsDeleted = 'false' ORDER BY Name")
        if shelves_res and shelves_res[1] then
            for _, shelf_name in ipairs(shelves_res[1]) do
                local books_res = queryAll(conn,
                    "SELECT ContentId FROM ShelfContent " ..
                    "WHERE ShelfName = ? AND _IsDeleted = 'false'",
                    shelf_name)
                if books_res and books_res[1] then
                    local coll_name = SHELF_PREFIX .. shelf_name
                    for _, book_id in ipairs(books_res[1]) do
                        if book_id and book_id ~= "" then
                            local file_path = KEPUB_DIR .. "/" .. book_id
                            if lfs.attributes(file_path, "mode") == "file" then
                                addDesired(file_path, coll_name)
                            end
                        end
                    end
                end
            end
        end

        local series_res = queryAll(conn,
            "SELECT ContentID, Series FROM content " ..
            "WHERE ContentType=6 AND Series IS NOT NULL AND Series != '' " ..
            "ORDER BY Series, CAST(SeriesNumber AS REAL)")
        if series_res and series_res[1] then
            for i, book_id in ipairs(series_res[1]) do
                local series = series_res[2] and series_res[2][i]
                if book_id and book_id ~= "" and series and series ~= "" then
                    local file_path = KEPUB_DIR .. "/" .. book_id
                    if lfs.attributes(file_path, "mode") == "file" then
                        addDesired(file_path, SERIES_PREFIX .. series)
                    end
                end
            end
        end
        return true
    end)

    if not ok then
        showInfo(_("Could not open Kobo database."), 3)
        return
    end

    for coll_name, coll in pairs(ReadCollection.coll) do
        if isSyncedCollectionName(coll_name) then
            local keep = desired[coll_name] or {}
            for file_path in pairs(coll) do
                if not keep[file_path] then
                    coll[file_path] = nil
                    removed = removed + 1
                    changed = true
                end
            end
        end
    end

    if changed then
        ReadCollection:write(nil)
    end

    showInfo(T(
        _("Sync complete.\n%1 added, %2 removed, %3 already present."),
        added, removed, skipped
    ), 4)
end

local function readApiEndpoint()
    return readConfValue("OneStoreServices", "api_endpoint")
end

local function writeApiEndpoint(new_url)
    return writeConfValue("OneStoreServices", "api_endpoint", new_url, false)
end

local function showSyncServerDialog()
    local current = readApiEndpoint()
    if not current then
        showInfo(_("Could not read Kobo eReader.conf"), 3)
        return
    end

    local dialog
    dialog = InputDialog:new{
        title       = _("Kobo Sync Server - api_endpoint"),
        input       = current,
        input_hint  = _("http://your-server/api/kobo/token"),
        buttons     = {
            {
                {
                    text     = _("Cancel"),
                    id       = "close",
                    callback = function()
                        UIManager:close(dialog)
                    end,
                },
                {
                    text     = _("Reset"),
                    callback = function()
                        UIManager:close(dialog)
                        UIManager:show(ConfirmBox:new{
                            text        = _("Reset sync server to Kobo default?\n\nThis will restore:\n") .. KOBO_DEFAULT_ENDPOINT,
                            ok_text     = _("Reset"),
                            cancel_text = _("Cancel"),
                            ok_callback = function()
                                local ok2, err = writeApiEndpoint(KOBO_DEFAULT_ENDPOINT)
                                if ok2 then
                                    showInfo(_("Sync server reset to Kobo default.\nTake effect on next Nickel sync."), 3)
                                else
                                    showInfo(_("Failed to reset:\n") .. tostring(err), 4)
                                end
                            end,
                        })
                    end,
                },
                {
                    text             = _("Save"),
                    is_enter_default = true,
                    callback         = function()
                        local new_url = dialog:getInputText():match("^%s*(.-)%s*$")
                        if new_url == "" then
                            showInfo(_("URL cannot be empty."), 2)
                            return
                        end
                        UIManager:close(dialog)
                        local ok2, err = writeApiEndpoint(new_url)
                        if ok2 then
                            showInfo(_("Sync server updated.\nTake effect on next Nickel sync."), 3)
                        else
                            showInfo(_("Failed to save:\n") .. tostring(err), 4)
                        end
                    end,
                },
            },
        },
    }
    UIManager:show(dialog)
    dialog:onShowKeyboard()
end

local function rssToggleItem(text, key)
    return {
        text         = text,
        checked_func = function() return rss[key] end,
        callback     = function()
            rss[key] = not rss[key]
            saveRSS()
        end,
        keep_menu_open = true,
    }
end

local function rssDirectionMenu(text, key)
    local function choice(label, value)
        return {
            text         = label,
            checked_func = function() return rss[key] == value end,
            radio        = true,
            callback     = function()
                rss[key] = value
                saveRSS()
            end,
            keep_menu_open = true,
        }
    end

    return {
        text           = text,
        sub_item_table = {
            choice(_("Silent"), DIRECTION.SILENT),
            choice(_("Prompt"), DIRECTION.PROMPT),
            choice(_("Never"), DIRECTION.NEVER),
        },
    }
end

local function progressSyncMenu()
    return {
        text_func = function()
            return rss.enabled
                and _("Kobo Progress Auto-Sync (on)")
                or  _("Kobo Progress Auto-Sync (off)")
        end,
        sub_item_table = {
            {
                text_func    = function() return rss.enabled and _("Enabled") or _("Disabled") end,
                checked_func = function() return rss.enabled end,
                callback     = function()
                    rss.enabled = not rss.enabled
                    saveRSS()
                end,
                keep_menu_open = true,
            },
            rssToggleItem(_("Auto-sync on book open"), "auto_sync_on_open"),
            rssToggleItem(_("Auto-sync on book close"), "auto_sync_on_close"),
            rssDirectionMenu(_("Push to Kobo"), "sync_to_kobo"),
            rssDirectionMenu(_("Pull from Kobo"), "sync_from_kobo"),
            {
                text     = _("Sync All Progress"),
                callback = syncAllBooks,
            },
        },
    }
end

local function showKepubHomeRestartPrompt()
    local enabling = not isKepubHomeEnabled()
    setKepubHome(enabling)
    local msg = enabling
        and _("Home folder set to Kepub library.")
        or  _("Home folder reset to Onboard.")
    UIManager:show(ConfirmBox:new{
        text        = msg .. "\n\n" .. _("Restart KOReader now to apply?"),
        ok_text     = _("Restart"),
        cancel_text = _("Later"),
        ok_callback = function() UIManager:restartKOReader() end,
    })
end

local function clearSyncedCollections()
    UIManager:show(ConfirmBox:new{
        text        = _("Remove all ◆ shelf and ☆ series collections?"),
        ok_text     = _("Clear"),
        cancel_text = _("Cancel"),
        ok_callback = function()
            local to_remove = {}
            for coll_name in pairs(ReadCollection.coll) do
                if isSyncedCollectionName(coll_name) then
                    to_remove[#to_remove + 1] = coll_name
                end
            end
            for _, coll_name in ipairs(to_remove) do
                ReadCollection:removeCollection(coll_name)
            end
            if #to_remove > 0 then
                ReadCollection:write(nil)
            end
            showInfo(T(_("Removed %1 collections."), #to_remove), 3)
        end,
    })
end

function KoboInt:addToMainMenu(menu_items)

    menu_items.koboko = {
        text         = _("Kobo Integration"),
        sorting_hint = "tools",
        sub_item_table = {
            {
                text     = _("Kobo Library Sync"),
                callback = doNickelSync,
            },
            {
                text     = _("Kobo Collection & Series Sync"),
                callback = syncKoboCollections,
            },
            progressSyncMenu(),
            {
                text           = _("Kobo Settings"),
                sub_item_table = {
                    {
                        text         = _("Kobo Set Home"),
                        checked_func = function() return isKepubHomeEnabled() end,
                        callback     = showKepubHomeRestartPrompt,
                        keep_menu_open = true,
                    },
                    {
                        text     = _("Kobo Sync Server"),
                        callback = showSyncServerDialog,
                    },
                    {
                        text     = _("Clear Synced Collections"),
                        callback = clearSyncedCollections,
                    },
                },
            },
        },
    }
end

return KoboInt
