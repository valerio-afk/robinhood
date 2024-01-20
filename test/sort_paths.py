from os.path import split

x = [
'../Code/test/__init__.py',
'../Code/test/__pycache__',
'../Code/test/__pycache__/NTPathTests.cpython-310.pyc',
'../Code/test/__pycache__/PosixPathTests.cpython-310.pyc',
'../Code/text_app.py',
'../Code/__init__.py',
'../Code/__pycache__',
'../Code/__pycache__/backend.cpython-310.pyc',
'../Code/__pycache__/commands.cpython-310.pyc',
'../Code/__pycache__/config.cpython-310.pyc',
'../Code/__pycache__/enums.cpython-310.pyc',
'../Code/__pycache__/filesystem.cpython-310.pyc',
'../Code/__pycache__/filesystem.cpython-39.pyc',
'../Code/__pycache__/file_filters.cpython-310.pyc',
'../Code/__pycache__/text_app.cpython-310.pyc',
'../Take',
'../Take/2023-10-08 01-30-35.mkv',
]

y = sorted(x,key=lambda path: (len(split(path)),path) )

for itm in y:
    print(itm)