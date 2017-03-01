var testName = "javascript";

var uris = cts.uris("", null, cts.directoryQuery(fn.concat("/testing/", testName, "/"), "infinity"));

fn.insertBefore(uris, 0, uris.count);
