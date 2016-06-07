# How to contribute to BigGraphite

First thanks :heart: :heart: :heart:

This document explains how to contribute code or bug reports, but feel free to ask your questions on [Gitter](https://gitter.im/criteo/biggraphite) or our [mailing list](https://groups.google.com/forum/#!forum/biggraphite).


## Did you find a bug?

- Ensure the bug was not already reported by searching on GitHub under [Issues](https://github.com/criteo/biggraphite/issues).

- If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/criteo/biggraphite/issues/new). Be sure to include a *title and clear description*, as much relevant information as possible.

## Code contributions

### Style guide

By convention:
- This project follows (PEP 8)[https://www.python.org/dev/peps/pep-0008/]
  and (PEP 257)[https://www.python.org/dev/peps/pep-0257/]
- We indent by four space
- We only import modules (`from os import path as os_path` not `from os.path import join`).

### Did you write a patch that fixes a bug?

- Check information on style above, `pylama tests biggraphite` will tell you about many violations
- Open a new GitHub pull request with the patch.
- Ensure the PR description clearly describes the problem and solution. Include the relevant issue number if applicable.

### Do you intend to add a new feature or change an existing one?

- Check information on style above.
- Suggest your change in the [mailing list](https://groups.google.com/forum/#!forum/biggraphite) and start writing code.
- Do not open an issue on GitHub until you have collected positive feedback about the change. GitHub issues are primarily intended for bug reports and fixes.

### Running tests

To run tests you can use `tox`:
```bash
    pip install tox
    tox
```
