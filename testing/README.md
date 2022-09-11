## Setup instructions for running unit testing examples

Follow the setup instructions under Environment setup and Running Spark locally in the root [README](https://github.com/gizm00/oreilly_dataeng_book)

### After you've setup the environment

1. `cd` to this directory
2. `pyenv activate <your_venv_name>`
2. `pytest -v <test_file>::<test_to_execute>`

For example, if you want to run the `test_transform_manual` test in `test_transform.py`:  
`pytest -v test_transform.py::test_transform_manual`
