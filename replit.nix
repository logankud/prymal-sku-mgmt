{pkgs}: {
  deps = [
    pkgs.netcat
    pkgs.rustc
    pkgs.libiconv
    pkgs.cargo
    pkgs.cacert
    pkgs.libxcrypt
    pkgs.glibcLocales
  ];
}
