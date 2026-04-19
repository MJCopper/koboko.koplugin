local _ = require("gettext")

return {
    id          = "koboko.koplugin",
    name        = "koboko",
    fullname    = _("Kobo KOReader Integration"),
    description = _("Kepub browsing, bidirectional reading state sync, Nickel library sync, collection sync, and sync server config for Kobo."),
    author      = "MJCopper",
    version     = "0.0.1",
    supported_platforms = { "kobo" },
}
