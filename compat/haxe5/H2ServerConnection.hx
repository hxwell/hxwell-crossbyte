package;

/** Haxe 4 permitted an imported module prefix for the secondary interface.
    Haxe 5 resolves it as a module path; retain the same original types. */
typedef H2ServerConnection = crossbyte._internal.http.h2.H2ServerConnection;
typedef H2FrameSink = crossbyte._internal.http.h2.H2ServerConnection.H2FrameSink;
