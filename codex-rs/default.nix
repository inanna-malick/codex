{
  cmake,
  fetchurl,
  llvmPackages,
  openssl,
  libcap ? null,
  rustPlatform,
  pkg-config,
  lib,
  stdenv,
  version ? "0.0.0",
  ...
}:
let
  rustyV8Archives = {
    "aarch64-darwin" = {
      bindingFileName = "src_binding_ptrcomp_sandbox_release_aarch64-apple-darwin.rs";
      bindingHash = "sha256-ylrfDPicmnCtRgrnNkiy/om3SqETs8t/dXtqArdYOU8=";
      fileName = "librusty_v8_ptrcomp_sandbox_release_aarch64-apple-darwin.a.gz";
      hash = "sha256-AK27SHmISMd1UEQcaGc6XoUpuOG3PqvN7iMss5tA9KE=";
    };
    "aarch64-linux" = {
      bindingFileName = "src_binding_ptrcomp_sandbox_release_aarch64-unknown-linux-gnu.rs";
      bindingHash = "sha256-dyeCauR5vbZF6Acjn7EtH44uI956bPFvXuWSaQ0dhQY=";
      fileName = "librusty_v8_ptrcomp_sandbox_release_aarch64-unknown-linux-gnu.a.gz";
      hash = "sha256-0VF+7UBUaFNwKbAF1f6ZfsdNXI01H5FrOm3yC30oEbo=";
    };
    "x86_64-darwin" = {
      bindingFileName = "src_binding_ptrcomp_sandbox_release_x86_64-apple-darwin.rs";
      bindingHash = "sha256-ylrfDPicmnCtRgrnNkiy/om3SqETs8t/dXtqArdYOU8=";
      fileName = "librusty_v8_ptrcomp_sandbox_release_x86_64-apple-darwin.a.gz";
      hash = "sha256-4Nm7ZOizoDTCkwyDly8/NXYCERSDQvoEB7OCUO8zCFY=";
    };
    "x86_64-linux" = {
      bindingFileName = "src_binding_ptrcomp_sandbox_release_x86_64-unknown-linux-gnu.rs";
      bindingHash = "sha256-dyeCauR5vbZF6Acjn7EtH44uI956bPFvXuWSaQ0dhQY=";
      fileName = "librusty_v8_ptrcomp_sandbox_release_x86_64-unknown-linux-gnu.a.gz";
      hash = "sha256-o1x10fJuapg4haRbM0kKTr5U8FBQVosyuJz7QhswtYM=";
    };
  };
  rustyV8ArchiveSpec =
    rustyV8Archives.${stdenv.hostPlatform.system}
      or (throw "codex-code-mode-host has no prebuilt rusty_v8 archive for ${stdenv.hostPlatform.system}");
  rustyV8Archive = fetchurl {
    url = "https://github.com/openai/codex/releases/download/rusty-v8-v150.4.0/${rustyV8ArchiveSpec.fileName}";
    inherit (rustyV8ArchiveSpec) hash;
  };
  rustyV8Binding = fetchurl {
    url = "https://github.com/openai/codex/releases/download/rusty-v8-v150.4.0/${rustyV8ArchiveSpec.bindingFileName}";
    hash = rustyV8ArchiveSpec.bindingHash;
  };
in
rustPlatform.buildRustPackage (_: {
  env = {
    PKG_CONFIG_PATH = lib.makeSearchPathOutput "dev" "lib/pkgconfig" (
      [ openssl ] ++ lib.optionals stdenv.isLinux [ libcap ]
    );
    RUSTY_V8_ARCHIVE = rustyV8Archive;
    RUSTY_V8_SRC_BINDING_PATH = rustyV8Binding;
  };
  pname = "codex-rs";
  inherit version;
  cargoLock.lockFile = ./Cargo.lock;
  cargoBuildFlags = [
    "-p"
    "codex-cli"
    "--bin"
    "codex"
    "-p"
    "codex-code-mode-host"
    "--bin"
    "codex-code-mode-host"
  ];
  doCheck = false;
  doInstallCheck = true;
  installCheckPhase = ''
    test -x "$out/bin/codex"
    test -x "$out/bin/codex-code-mode-host"
    "$out/bin/codex" --help > /dev/null
    "$out/bin/codex-code-mode-host" --help > /dev/null
    "$out/bin/codex" queue --help > /dev/null
    "$out/bin/codex" --host-dynamic-tools-socket /tmp/codex-host-dynamic-tools.sock --help > /dev/null
    "$out/bin/codex" --help | grep -q -- "--host-dynamic-tools-socket"
  '';
  src = ./.;

  # Patch the workspace Cargo.toml so that cargo embeds the correct version in
  # CARGO_PKG_VERSION (which the binary reads via env!("CARGO_PKG_VERSION")).
  # On release commits the Cargo.toml already contains the real version and
  # this sed is a no-op.
  postPatch = ''
    sed -i 's/^version = "0\.0\.0"$/version = "${version}"/' Cargo.toml
  '';
  nativeBuildInputs = [
    cmake
    llvmPackages.clang
    llvmPackages.libclang.lib
    openssl
    pkg-config
  ] ++ lib.optionals stdenv.isLinux [
    libcap
  ];

  cargoLock.outputHashes = {
    "appcontainer_common-0.8.0" = "sha256-XUkT2R+RYk9WIqgKnmIAagNW4xOTyp4bWHmQL1iznHw=";
    "crossterm-0.29.0" = "sha256-cQxQQuV+YEutuQiPurXVISq6F/99vCEk8qe5PU8BCSo=";
    "learning_mode_core-0.8.0" = "sha256-XUkT2R+RYk9WIqgKnmIAagNW4xOTyp4bWHmQL1iznHw=";
    "learning_mode_windows-0.8.0" = "sha256-XUkT2R+RYk9WIqgKnmIAagNW4xOTyp4bWHmQL1iznHw=";
    "mxc_config_contract-0.8.0" = "sha256-XUkT2R+RYk9WIqgKnmIAagNW4xOTyp4bWHmQL1iznHw=";
    "mxc_telemetry-0.8.0" = "sha256-XUkT2R+RYk9WIqgKnmIAagNW4xOTyp4bWHmQL1iznHw=";
    "nucleo-0.5.0" = "sha256-Hm4SxtTSBrcWpXrtSqeO0TACbUxq3gizg1zD/6Yw/sI=";
    "nucleo-matcher-0.3.1" = "sha256-Hm4SxtTSBrcWpXrtSqeO0TACbUxq3gizg1zD/6Yw/sI=";
    "process_security_environment_spec-0.8.0" = "sha256-XUkT2R+RYk9WIqgKnmIAagNW4xOTyp4bWHmQL1iznHw=";
    "runfiles-0.1.0" = "sha256-uJpVLcQh8wWZA3GPv9D8Nt43EOirajfDJ7eq/FB+tek=";
    "sandbox_spec-0.8.0" = "sha256-XUkT2R+RYk9WIqgKnmIAagNW4xOTyp4bWHmQL1iznHw=";
    "tokio-tungstenite-0.28.0" = "sha256-V1xmnrfRWOcZZogelZEA4vvyMj2awCfHVA5/glQ6KAI=";
    "tungstenite-0.27.0" = "sha256-VVHhk7l9J/sEmG3q/UuV/sQ3f+fGsmq5vumSy8vbMvw=";
    "wxc_common-0.8.0" = "sha256-XUkT2R+RYk9WIqgKnmIAagNW4xOTyp4bWHmQL1iznHw=";
  };

  meta = with lib; {
    description = "OpenAI Codex command‑line interface rust implementation";
    license = licenses.asl20;
    homepage = "https://github.com/openai/codex";
    mainProgram = "codex";
  };
})
