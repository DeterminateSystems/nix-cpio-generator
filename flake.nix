{
  description = "nix-cpio-generator";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    cpiotools.url = "github:DeterminateSystems/cpiotools";
  };

  outputs =
    { self
    , nixpkgs
    , cpiotools
    , ...
    } @ inputs:
    let
      nameValuePair = name: value: { inherit name value; };
      genAttrs = names: f: builtins.listToAttrs (map (n: nameValuePair n (f n)) names);
      allSystems = [ "x86_64-linux" "aarch64-linux" "i686-linux" "x86_64-darwin" ];

      forAllSystems = f: genAttrs allSystems (system: f {
        inherit system;
        pkgs = import nixpkgs { inherit system; };
      });
    in
    {
      devShell = forAllSystems ({ system, pkgs, ... }: self.packages.${system}.package.overrideAttrs ({ nativeBuildInputs ? [ ], ... }: {
        nativeBuildInputs = nativeBuildInputs ++ (with pkgs; [
          binwalk
          codespell
          entr
          file
          nixpkgs-fmt
          rustfmt
          vim # xxd
          cpiotools.packages.${system}.package
          cargo
        ]);
      }));

      packages = forAllSystems
        ({ system, pkgs, ... }: {
          package = pkgs.hello;
        });
    };
}
