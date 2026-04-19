--[[
    koboko — Kobo/KOReader integration plugin

    1. Kepub browsing     — exposes .kobo/kepub/ and opens kepubs as EPUBs
    2. Reading state sync — bidirectional KOReader ↔ Kobo SQLite progress
    3. Nickel library sync — exits to Nickel to trigger a book download sync
    4. Collection sync    — maps Kobo shelves/series to KOReader collections
    5. Sync server config — view/edit api_endpoint in Kobo eReader.conf
--]]

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

-- ═══════════════════════════════════════════════════════════════════════════
-- § 1  KEPUB BROWSING
-- ═══════════════════════════════════════════════════════════════════════════

-- True only for files living under .kobo/kepub/ with no extension.
local function is_kobo_kepub(path)
    if not path then return false end
    if not path:find("/%.kobo/kepub/") then return false end
    local basename = path:match("([^/]+)$") or ""
    return not basename:find("%.")
end

-- Bare kepub filename predicate used in splitFileNameType (receives a filename, not a path).
-- Kepub IDs are always numeric, so we require that to avoid false positives on
-- extensionless files like README or Makefile.
local function is_bare_no_ext(name)
    if not name or name == "" then return false end
    return name:match("^%d+$") ~= nil
end

-- Remove kepub from excluded dirs so FileChooser shows it.
do
    local new_exclude = {}
    for _, p in ipairs(FileChooser.exclude_dirs) do
        if p ~= "^kepub$" then new_exclude[#new_exclude + 1] = p end
    end
    FileChooser.exclude_dirs = new_exclude
end

-- DocumentRegistry patches — route kepub paths through the .epub provider.
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

-- getFileNameSuffix receives a full path; kepubs should report as epub.
local _orig_getFileNameSuffix = util.getFileNameSuffix
function util.getFileNameSuffix(file)
    if is_kobo_kepub(file) then return "epub" end
    return _orig_getFileNameSuffix(file)
end

-- splitFileNameType receives a bare filename (path already stripped), so we use
-- is_bare_no_ext. For kepub numeric IDs we return the book title from the Kobo DB
-- so CoverBrowser shows a readable name instead of the raw ID.
-- openDB/SYNC_DB are resolved at call time.
local _orig_splitFileNameType = filemanagerutil.splitFileNameType
function filemanagerutil.splitFileNameType(filename)
    if is_bare_no_ext(filename) then
        local title
        pcall(function()
            local conn = openDB()
            local r = conn:exec(string.format(
                "SELECT Title FROM content WHERE ContentID='%s' AND ContentType=6 LIMIT 1",
                filename))
            if r and r[1] and r[1][1] and r[1][1] ~= "" then
                title = r[1][1]
            end
            conn:close()
        end)
        return title or filename, "epub"
    end
    return _orig_splitFileNameType(filename)
end

-- Kepubs are extensionless, so generate a unique sidecar name per book (e.g. metadata.1302.lua).
local _orig_getSidecarFilename = DocSettings.getSidecarFilename
function DocSettings.getSidecarFilename(doc_path)
    if is_kobo_kepub(doc_path) then
        local basename = doc_path:match("([^/]+)$") or doc_path
        return "metadata." .. basename .. ".lua"
    end
    return _orig_getSidecarFilename(doc_path)
end

-- Guard against nil cover dimensions in BookInfoManager to prevent a crash
-- when kepubs haven't had their covers cached yet.
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

-- Default metadata search to on. Not persisted by KOReader, so we set it here
-- each session. Only active when CoverBrowser is enabled.
do
    local ok_fs, FileSearcher = pcall(require, "apps/filemanager/filemanagerfilesearcher")
    if ok_fs and FileSearcher then
        FileSearcher.include_metadata = true
    end
end

-- ═══════════════════════════════════════════════════════════════════════════
-- § 2  READING STATE SYNC
-- ═══════════════════════════════════════════════════════════════════════════

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

local function openDB()
    return SQ3.open(SYNC_DB)
end

-- Kobo ReadStatus: 0=unopened, 1=reading, 2=finished
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

-- Kobo timestamps are UTC ISO 8601 ("2026-04-04T23:46:07Z"). os.time() treats
-- a table as local time, so we subtract the UTC offset to get the correct epoch.
local _utc_offset = os.time() - os.time(os.date("!*t"))
local function parseKoboTS(s)
    if not s or s == "" then return 0 end
    local y, mo, d, h, mi, sec = s:match("(%d+)-(%d+)-(%d+)[T ](%d+):(%d+):(%d+)")
    if not y then return 0 end
    local t = os.time({year=tonumber(y), month=tonumber(mo), day=tonumber(d),
                       hour=tonumber(h), min=tonumber(mi), sec=tonumber(sec)})
    return t and (t - _utc_offset) or 0
end

-- Kepub files are extension-less numeric IDs under /kepub/ (e.g. /kepub/1218).
local function extractBookId(path)
    if not path then return nil end
    return path:match("/kepub/(%d+)$")
end

-- Returns the KOReader last-read timestamp only if a sidecar exists;
-- a missing sidecar means the book has never been opened in KOReader.
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

-- ContentID for kepubs is the bare numeric ID. ___PercentRead on ContentType=6
-- is the reliable overall progress value (0-100).
local function readKoboState(book_id)
    local conn = openDB()
    if not conn then return nil end
    local res = conn:exec(string.format(
        "SELECT DateLastRead, ReadStatus, ___PercentRead " ..
        "FROM content WHERE ContentID='%s' AND ContentType=6 LIMIT 1",
        book_id))
    if not res or not res[1] or not res[1][1] then
        conn:close()
        return nil
    end
    local date_lr = res[1][1]
    local read_st = tonumber((res[2] and res[2][1]) or 0)
    local pct     = tonumber((res[3] and res[3][1]) or 0)
    if read_st == 2 and pct == 0 then pct = 100 end
    conn:close()
    return {
        percent_read = pct,
        timestamp    = parseKoboTS(date_lr),
        status       = koboToKR(read_st),
        kobo_status  = read_st,
    }
end

-- Chapter ContentIDs use "!" as separator: "1148!OEBPS!Text/chapter02.xhtml"
-- ChapterIDBookmarked: strip the book ID prefix, replace "!" with "/", append "#"
--   e.g. "1148!OEBPS!Text/chapter02.xhtml" → "OEBPS/Text/chapter02.xhtml#"
-- ___FileOffset/___FileSize are float percentages (0-100).
local function writeKoboState(book_id, pct, ts, kr_status)
    -- "abandoned" (On hold) has no Kobo equivalent — don't overwrite Kobo state
    if kr_status == "abandoned" then return false end
    local conn = openDB()
    if not conn then return false end
    local date_str = ts and ts > 0 and os.date("!%Y-%m-%dT%H:%M:%SZ", ts) or ""
    local read_st  = krToKobo(kr_status)
    pct = tonumber(pct) or 0
    local pct_int = math.floor(pct)

    local ch_bm = ""

    local cr = conn:exec(string.format(
        "SELECT ContentID, ___FileOffset, ___FileSize FROM content " ..
        "WHERE ContentID LIKE '%s%%' AND ContentType=9 " ..
        "AND ___FileOffset <= %f ORDER BY ___FileOffset DESC LIMIT 1",
        book_id, pct))
    if cr and cr[1] and cr[1][1] then
        local ch_cid  = cr[1][1]
        local ch_off  = tonumber(cr[2][1]) or 0
        local ch_size = tonumber(cr[3][1]) or 0
        local ch_end  = ch_off + ch_size

        if pct > ch_end then
            local last = conn:exec(string.format(
                "SELECT ContentID FROM content WHERE ContentID LIKE '%s%%' " ..
                "AND ContentType=9 ORDER BY ___FileOffset DESC LIMIT 1",
                book_id))
            if last and last[1] and last[1][1] then
                ch_cid = last[1][1]
                ch_size = 0
            end
        end

        local ch_pct = ch_size > 0 and ((pct - ch_off) / ch_size) * 100 or 0
        ch_pct = math.max(0, math.min(100, ch_pct))

        pcall(function()
            local s = conn:prepare(
                "UPDATE content SET ___PercentRead=? WHERE ContentID=? AND ContentType=9")
            s:bind(math.floor(ch_pct), ch_cid)
            s:step()
            s:close()
        end)

        -- Strip the book ID prefix and convert "!" separators to "/" for ChapterIDBookmarked.
        local stripped = ch_cid:match("^[^!]+!(.+)$")
        if stripped and stripped ~= "" then
            ch_bm = stripped:gsub("!", "/") .. "#"
        end
    end

    local ok2 = pcall(function()
        local s = conn:prepare(
            "UPDATE content SET ___PercentRead=?, DateLastRead=?, " ..
            "ReadStatus=?, ChapterIDBookmarked=? " ..
            "WHERE ContentID=? AND ContentType=6")
        s:bind(pct_int, date_str, read_st, ch_bm, book_id)
        s:step()
        s:close()
    end)
    conn:close()
    return ok2
end

local function decide(dir_setting, is_pull, fn, details)
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
        return ok3
    end
    UIManager:show(ConfirmBox:new{
        text        = text,
        ok_text     = _("Yes"),
        cancel_text = _("No"),
        ok_callback = function() if fn then fn() end end,
    })
    return true
end

-- Falls back to the Kobo DB title if doc_settings has none, then to the book ID.
local function getBookTitle(book_id, doc_settings)
    local title = doc_settings and doc_settings:readSetting("title")
    if title and title ~= "" then return title end
    local conn = openDB()
    if conn then
        local res = conn:exec(string.format(
            "SELECT Title FROM content WHERE ContentID='%s' AND ContentType=6 LIMIT 1",
            book_id))
        conn:close()
        if res and res[1] and res[1][1] and res[1][1] ~= "" then
            return res[1][1]
        end
    end
    return book_id
end

-- Bidirectional sync for one book — most recently read side wins.
local function syncOneBook(book_id, doc_settings, doc_path)
    local kobo = readKoboState(book_id)
    if not kobo then return false end
    local kr_pct  = doc_settings:readSetting("percent_finished") or 0
    local kr_ts   = getKRTimestamp(doc_path)
    local summary = doc_settings:readSetting("summary") or {}
    local kr_st   = summary.status or "reading"
    local kobo_done = kobo.status == "complete" or kobo.percent_read >= 100
    local kr_done   = kr_pct >= 1.0 or kr_st == "complete" or kr_st == "finished"
    -- Skip only when both sides are done and agree on status. If they disagree
    -- (e.g. one is "complete" and the other isn't) we still need to push the state.
    if kobo_done and kr_done then
        local kobo_complete = kobo.status == "complete"
        local kr_complete   = kr_st == "complete" or kr_st == "finished"
        if kobo_complete == kr_complete then return false end
    end
    local title = getBookTitle(book_id, doc_settings)
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
            writeKoboState(book_id, kr_pct * 100, os.time(), kr_st)
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
        for i, book in ipairs(books) do
            if not Trapper:info(T(_("Syncing: %1 / %2"), i, #books)) then
                Trapper:clear()
                return
            end
            local ds = DocSettings:open(book.path)
            if ds then
                total = total + 1
                if syncOneBook(book.id, ds, book.path) then
                    synced = synced + 1
                end
            end
        end
        ffiutil.sleep(1)
        Trapper:info(T(_("Synced %1 of %2 books"), synced, total))
        ffiutil.sleep(2)
        Trapper:clear()
    end)
end

-- Patch abbreviate to show the book title instead of the numeric kepub ID.
-- Defined here (after §2) because it closes over openDB and SYNC_DB.
do
    local _orig_abbreviate = filemanagerutil.abbreviate
    filemanagerutil.abbreviate = function(path)
        if is_kobo_kepub(path) then
            local book_id = path:match("/kepub/(%d+)$")
            if book_id then
                local title
                pcall(function()
                    local conn = openDB()
                    local r = conn:exec(string.format(
                        "SELECT Title FROM content WHERE ContentID='%s' AND ContentType=6 LIMIT 1",
                        book_id))
                    if r and r[1] and r[1][1] and r[1][1] ~= "" then
                        title = r[1][1]
                    end
                    conn:close()
                end)
                if title then return title end
            end
        end
        return _orig_abbreviate(path)
    end
end

-- ═══════════════════════════════════════════════════════════════════════════
-- § 3  NICKEL LIBRARY SYNC
-- ═══════════════════════════════════════════════════════════════════════════

local function setSyncOnNextBoot()
    local f = io.open(KOBO_CONF, "r")
    if not f then
        return false, "Could not open Kobo eReader.conf"
    end
    local lines = {}
    for line in f:lines() do
        table.insert(lines, line)
    end
    f:close()

    local found = false
    for i, line in ipairs(lines) do
        if line:match("^syncOnNextBoot=") then
            lines[i] = "syncOnNextBoot=true"
            found = true
            break
        end
    end

    if not found then
        for i, line in ipairs(lines) do
            if line:match("^%[OneStoreServices%]") then
                table.insert(lines, i + 1, "syncOnNextBoot=true")
                found = true
                break
            end
        end
    end

    if not found then
        return false, "[OneStoreServices] section not found in Kobo eReader.conf"
    end

    local out = io.open(KOBO_CONF, "w")
    if not out then
        return false, "Could not write Kobo eReader.conf"
    end
    for _, line in ipairs(lines) do
        out:write(line .. "\n")
    end
    out:close()
    return true
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
                UIManager:show(InfoMessage:new{
                    text = _("Please wait\xe2\x80\xa6"),
                    icon = "notice-info",
                })
                UIManager:forceRePaint()
                UIManager:scheduleIn(1.5, function()
                    UIManager:quit(0)
                end)
            else
                UIManager:show(InfoMessage:new{
                    text    = _("Failed to schedule sync:\n") .. tostring(err),
                    timeout = 3,
                })
            end
        end,
    })
end

-- ═══════════════════════════════════════════════════════════════════════════
-- § 4  PLUGIN WIDGET (menu integration)
-- ═══════════════════════════════════════════════════════════════════════════

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

function KoboInt:init()
    self.ui.menu:registerToMainMenu(self)
    -- Re-apply home_dir on startup to guard against it being changed externally.
    if isKepubHomeEnabled() and self.ui.name == "filemanager" then
        G_reader_settings:saveSetting("home_dir", KEPUB_DIR)
    end
end

-- Fires while the document is still live, so doc_settings is valid. We capture
-- state here but defer the DB write one tick so the close sequence (including
-- doc_settings flush) finishes first. Push only — no pull, because ReadHistory
-- hasn't been updated yet so we can't do a fair timestamp comparison.
function KoboInt:onCloseDocument()
    if not rss.enabled or not rss.auto_sync_on_close then return end
    if not self.ui.document or not self.ui.doc_settings then return end

    local path    = self.ui.document.file
    local book_id = extractBookId(path)
    if not book_id then return end

    local kr_pct = self.ui.doc_settings:readSetting("percent_finished") or 0
    local summary = self.ui.doc_settings:readSetting("summary") or {}
    local kr_st  = summary.status or "reading"

    -- Skip if already complete on both sides.
    local kr_done = kr_pct >= 1.0 or kr_st == "complete" or kr_st == "finished"
    if kr_done then
        local kobo = readKoboState(book_id)
        if kobo and kobo.status == "complete" then
            return
        end
        -- Kobo not yet complete — fall through to push the finished status.
    end

    UIManager:scheduleIn(0, function()
        decide(rss.sync_to_kobo, false, function()
            writeKoboState(book_id, kr_pct * 100, os.time(), kr_st)
        end)
    end)
end

-- Pull Kobo → KOReader on book open. Kobo wins if it's ahead.
function KoboInt:onReaderReady()
    if not rss.enabled or not rss.auto_sync_on_open then return end
    if not self.ui.document or not self.ui.doc_settings then return end

    local path    = self.ui.document.file
    local book_id = extractBookId(path)
    if not book_id then return end

    local kobo = readKoboState(book_id)
    if not kobo then return end

    -- Never opened in Kobo — nothing to pull.
    if kobo.kobo_status == 0 and kobo.percent_read == 0 then return end

    local kr_pct = self.ui.doc_settings:readSetting("percent_finished") or 0
    local summary = self.ui.doc_settings:readSetting("summary") or {}
    local kr_st  = summary.status or "reading"

    local kobo_done = kobo.status == "complete" or kobo.percent_read >= 100
    local kr_done   = kr_pct >= 1.0 or kr_st == "complete" or kr_st == "finished"

    if kobo_done and kr_done then return end
    if not kobo_done and kr_done then return end   -- KOReader is ahead; push handles it on close
    if not kobo_done and not kr_done then
        if kobo.percent_read <= kr_pct * 100 then return end
    end
    -- Falls through only when Kobo is finished and KOReader isn't.

    local title = getBookTitle(book_id, self.ui.doc_settings)
    decide(rss.sync_from_kobo, true, function()
        local p = kobo.percent_read / 100.0
        self.ui.doc_settings:saveSetting("percent_finished", p)
        self.ui.doc_settings:saveSetting("last_percent", p)
        local s = self.ui.doc_settings:readSetting("summary") or {}
        s.status = kobo.status
        if kobo.percent_read >= 100 then s.status = "complete" end
        self.ui.doc_settings:saveSetting("summary", s)
        self.ui.doc_settings:flush()
        if self.ui.rolling then
            self.ui:handleEvent(Event:new("GotoPercent", p * 100))
        end
    end, {
        title     = title,
        src_pct   = kobo.percent_read,
        dst_pct   = kr_pct * 100,
        src_time  = kobo.timestamp,
        dst_time  = getKRTimestamp(path),
    })
end

-- ── Collection + series sync ───────────────────────────────────────────────

local function syncKoboCollections()
    local conn = openDB()
    if not conn then
        UIManager:show(InfoMessage:new{
            text    = _("Could not open Kobo database."),
            timeout = 3,
        })
        return
    end

    local added   = 0
    local skipped = 0

    -- Shelves → ◆ collections
    local shelves_res = conn:exec(
        "SELECT Name FROM Shelf WHERE _IsDeleted = 'false' ORDER BY Name")
    if shelves_res and shelves_res[1] then
        for _, shelf_name in ipairs(shelves_res[1]) do
            local books_res = conn:exec(string.format(
                "SELECT ContentId FROM ShelfContent " ..
                "WHERE ShelfName = '%s' AND _IsDeleted = 'false'",
                shelf_name:gsub("'", "''")))
            if books_res and books_res[1] then
                local coll_name = SHELF_PREFIX .. shelf_name
                if not ReadCollection.coll[coll_name] then
                    ReadCollection:addCollection(coll_name)
                end
                for _, book_id in ipairs(books_res[1]) do
                    if book_id and book_id ~= "" then
                        local file_path = KEPUB_DIR .. "/" .. book_id
                        if lfs.attributes(file_path, "mode") == "file" then
                            if not ReadCollection:isFileInCollection(file_path, coll_name) then
                                ReadCollection:addItem(file_path, coll_name)
                                added = added + 1
                            else
                                skipped = skipped + 1
                            end
                        end
                    end
                end
            end
        end
    end

    -- Series → ☆ collections
    local series_res = conn:exec(
        "SELECT ContentID, Series FROM content " ..
        "WHERE ContentType=6 AND Series IS NOT NULL AND Series != '' " ..
        "ORDER BY Series, CAST(SeriesNumber AS REAL)")
    if series_res and series_res[1] then
        for i, book_id in ipairs(series_res[1]) do
            local series = series_res[2] and series_res[2][i]
            if book_id and book_id ~= "" and series and series ~= "" then
                local file_path = KEPUB_DIR .. "/" .. book_id
                if lfs.attributes(file_path, "mode") == "file" then
                    local coll_name = SERIES_PREFIX .. series
                    if not ReadCollection.coll[coll_name] then
                        ReadCollection:addCollection(coll_name)
                    end
                    if not ReadCollection:isFileInCollection(file_path, coll_name) then
                        ReadCollection:addItem(file_path, coll_name)
                        added = added + 1
                    else
                        skipped = skipped + 1
                    end
                end
            end
        end
    end

    conn:close()

    if added > 0 then
        ReadCollection:write(nil)
    end

    UIManager:show(InfoMessage:new{
        text    = T(_("Sync complete.\n%1 added, %2 already present."), added, skipped),
        timeout = 4,
    })
end

local function readApiEndpoint()
    local f = io.open(KOBO_CONF, "r")
    if not f then
        return nil
    end
    local in_section = false
    for line in f:lines() do
        if line:match("^%[OneStoreServices%]") then
            in_section = true
        elseif line:match("^%[") then
            in_section = false
        elseif in_section then
            local val = line:match("^api_endpoint=(.+)$")
            if val then
                f:close()
                return val
            end
        end
    end
    f:close()
    return nil
end

local function writeApiEndpoint(new_url)
    local f = io.open(KOBO_CONF, "r")
    if not f then
        return false, "Could not open " .. KOBO_CONF
    end
    local lines = {}
    for line in f:lines() do
        table.insert(lines, line)
    end
    f:close()

    local found = false
    for i, line in ipairs(lines) do
        if line:match("^api_endpoint=") then
            lines[i] = "api_endpoint=" .. new_url
            found = true
            break
        end
    end

    if not found then
        return false, "api_endpoint key not found in conf"
    end

    local out = io.open(KOBO_CONF, "w")
    if not out then
        return false, "Could not write " .. KOBO_CONF
    end
    for _, line in ipairs(lines) do
        out:write(line .. "\n")
    end
    out:close()
    return true
end

local function showSyncServerDialog()
    local current = readApiEndpoint()
    if not current then
        UIManager:show(InfoMessage:new{
            text    = _("Could not read Kobo eReader.conf"),
            timeout = 3,
        })
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
                                    UIManager:show(InfoMessage:new{
                                        text    = _("Sync server reset to Kobo default.\nTake effect on next Nickel sync."),
                                        timeout = 3,
                                    })
                                else
                                    UIManager:show(InfoMessage:new{
                                        text    = _("Failed to reset:\n") .. tostring(err),
                                        timeout = 4,
                                    })
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
                            UIManager:show(InfoMessage:new{
                                text    = _("URL cannot be empty."),
                                timeout = 2,
                            })
                            return
                        end
                        UIManager:close(dialog)
                        local ok2, err = writeApiEndpoint(new_url)
                        if ok2 then
                            UIManager:show(InfoMessage:new{
                                text    = _("Sync server updated.\nTake effect on next Nickel sync."),
                                timeout = 3,
                            })
                        else
                            UIManager:show(InfoMessage:new{
                                text    = _("Failed to save:\n") .. tostring(err),
                                timeout = 4,
                            })
                        end
                    end,
                },
            },
        },
    }
    UIManager:show(dialog)
    dialog:onShowKeyboard()
end

-- ── Menu ───────────────────────────────────────────────────────────────────

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
            {
                text_func    = function()
                    return rss.enabled
                        and _("Kobo Progress Auto-Sync (on)")
                        or  _("Kobo Progress Auto-Sync (off)")
                end,
                sub_item_table = {
                    {
                        text_func    = function()
                            return rss.enabled and _("Enabled") or _("Disabled")
                        end,
                        checked_func = function() return rss.enabled end,
                        callback     = function()
                            rss.enabled = not rss.enabled
                            saveRSS()
                        end,
                        keep_menu_open = true,
                    },
                    {
                        text         = _("Auto-sync on book open"),
                        checked_func = function() return rss.auto_sync_on_open end,
                        callback     = function()
                            rss.auto_sync_on_open = not rss.auto_sync_on_open
                            saveRSS()
                        end,
                        keep_menu_open = true,
                    },
                    {
                        text         = _("Auto-sync on book close"),
                        checked_func = function() return rss.auto_sync_on_close end,
                        callback     = function()
                            rss.auto_sync_on_close = not rss.auto_sync_on_close
                            saveRSS()
                        end,
                        keep_menu_open = true,
                    },
                    {
                        text           = _("Push to Kobo"),
                        sub_item_table = {
                            {
                                text         = _("Silent"),
                                checked_func = function() return rss.sync_to_kobo == DIRECTION.SILENT end,
                                radio        = true,
                                callback     = function() rss.sync_to_kobo = DIRECTION.SILENT; saveRSS() end,
                                keep_menu_open = true,
                            },
                            {
                                text         = _("Prompt"),
                                checked_func = function() return rss.sync_to_kobo == DIRECTION.PROMPT end,
                                radio        = true,
                                callback     = function() rss.sync_to_kobo = DIRECTION.PROMPT; saveRSS() end,
                                keep_menu_open = true,
                            },
                            {
                                text         = _("Never"),
                                checked_func = function() return rss.sync_to_kobo == DIRECTION.NEVER end,
                                radio        = true,
                                callback     = function() rss.sync_to_kobo = DIRECTION.NEVER; saveRSS() end,
                                keep_menu_open = true,
                            },
                        },
                    },
                    {
                        text           = _("Pull from Kobo"),
                        sub_item_table = {
                            {
                                text         = _("Silent"),
                                checked_func = function() return rss.sync_from_kobo == DIRECTION.SILENT end,
                                radio        = true,
                                callback     = function() rss.sync_from_kobo = DIRECTION.SILENT; saveRSS() end,
                                keep_menu_open = true,
                            },
                            {
                                text         = _("Prompt"),
                                checked_func = function() return rss.sync_from_kobo == DIRECTION.PROMPT end,
                                radio        = true,
                                callback     = function() rss.sync_from_kobo = DIRECTION.PROMPT; saveRSS() end,
                                keep_menu_open = true,
                            },
                            {
                                text         = _("Never"),
                                checked_func = function() return rss.sync_from_kobo == DIRECTION.NEVER end,
                                radio        = true,
                                callback     = function() rss.sync_from_kobo = DIRECTION.NEVER; saveRSS() end,
                                keep_menu_open = true,
                            },
                        },
                    },
                    {
                        text     = _("Sync All Progress"),
                        callback = function() syncAllBooks() end,
                    },
                },
            },
            {
                text           = _("Kobo Settings"),
                sub_item_table = {
                    {
                        text         = _("Kobo Set Home"),
                        checked_func = function() return isKepubHomeEnabled() end,
                        callback     = function()
                            local enabling = not isKepubHomeEnabled()
                            setKepubHome(enabling)
                            local msg = enabling
                                and _("Home folder set to Kepub library.")
                                or  _("Home folder reset to Onboard.")
                            UIManager:show(ConfirmBox:new{
                                text        = msg .. "\n\n" .. _("Restart KOReader now to apply?"),
                                ok_text     = _("Restart"),
                                cancel_text = _("Later"),
                                ok_callback = function()
                                    UIManager:restartKOReader()
                                end,
                            })
                        end,
                        keep_menu_open = true,
                    },
                    {
                        text     = _("Kobo Sync Server"),
                        callback = showSyncServerDialog,
                    },
                    {
                        text     = _("Clear Synced Collections"),
                        callback = function()
                            UIManager:show(ConfirmBox:new{
                                text        = _("Remove all ◆ shelf and ☆ series collections?"),
                                ok_text     = _("Clear"),
                                cancel_text = _("Cancel"),
                                ok_callback = function()
                                    local to_remove = {}
                                    for coll_name in pairs(ReadCollection.coll) do
                                        local p = coll_name:sub(1, #SHELF_PREFIX)
                                        local q = coll_name:sub(1, #SERIES_PREFIX)
                                        if p == SHELF_PREFIX or q == SERIES_PREFIX then
                                            to_remove[#to_remove + 1] = coll_name
                                        end
                                    end
                                    for _, coll_name in ipairs(to_remove) do
                                        ReadCollection:removeCollection(coll_name)
                                    end
                                    if #to_remove > 0 then
                                        ReadCollection:write(nil)
                                    end
                                    UIManager:show(InfoMessage:new{
                                        text    = T(_("Removed %1 collections."), #to_remove),
                                        timeout = 3,
                                    })
                                end,
                            })
                        end,
                    },
                },
            },
        },
    }
end

return KoboInt
