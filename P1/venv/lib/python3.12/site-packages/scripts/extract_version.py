import toml


if __name__ == "__main__":
    with open("pyproject.toml", "r") as f:
        pyproject = toml.load(f)

    version = pyproject["project"]["version"]

    fp = "./scripts/__init__.py"
    with open(fp, "w") as version_file:
        version_file.write(f'__version__ = "{version}"\n')
