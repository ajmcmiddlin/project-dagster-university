{
  description = "Dagster university development";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # taken from https://wiki.nixos.org/wiki/Python#Prefix_library_paths_using_wrapProgram
        pythonldlibpath = pkgs.lib.makeLibraryPath (with pkgs; [
            zlib zstd stdenv.cc.cc curl openssl attr libssh bzip2 libxml2 acl libsodium util-linux xz systemd
          ]);

        # Darwin requires a different library path prefix
        wrapPrefix = if (pkgs.stdenv.isDarwin) then "DYLD_LIBRARY_PATH" else "LD_LIBRARY_PATH";
        patchedPython = (pkgs.symlinkJoin {
          name = "python";
          paths = [ pkgs.python312 ];
          buildInputs = [ pkgs.makeWrapper ];
          postBuild = ''
            wrapProgram "$out/bin/python3.12" --prefix ${wrapPrefix} : "${pythonldlibpath}"
          '';
        });
      in
      {
        dupkgs = pkgs;

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs;
            [
              patchedPython
              uv
              python312Packages.python-lsp-server
              duckdb
            ];

          shellHook = ''
            export SSL_CERT_FILE="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
          '';
        };
      });
}
