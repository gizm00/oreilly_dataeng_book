## Environment setup

1. Install [pyenv](https://github.com/pyenv/pyenv)
2. Install [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv)
3. Install python 3.8.5  
`pyenv install 3.8.5`
4. Create virtualenv  
`pyenv virtualenv 3.8.5 oreilly-book`  
5. Activate the virtual environment  
`pyenv activate oreilly-book`
6. Clone this repo  
`git clone git@github.com:gizm00/oreilly_dataeng_book.git`
7. `cd oreilly_dataeng_book`
8. `pip install wheel`
6. Install dependencies  
`python -m pip install -r requirements.txt`

### Running spark locally
(based on [these instructions](https://medium.com/tinghaochen/how-to-install-pyspark-locally-94501eefe421))  
Within the virtualenv created above run the following: 
1. Download [apache-spark](https://spark.apache.org/downloads.html) This material was developed using spark 3.2.1 with hadoop 3.2
2. Move the tgz file to a place you will refer to it from, i.e. ~/Development/
3. `tar -xvf ~/Development/spark-3.2.1-bin-hadoop3.2.tgz`
4. Add the following to your shell startup file, for example ~/.bash_profile:
```
export SPARK_HOME="/User/sev/Development/spark-3.2.1-bin-hadoop3.2"
export PATH="$SPARK_HOME/bin:$PATH"
```
5. `source ~/.bash_profile`
6. `pyspark`
  

If you use the VSCode IDE on OSX, you can run pyspark notebooks with [these instructions](https://8vi.cat/set-up-pyspark-in-mac-os-x-and-visual-studio-code/)  
* When you start the notebook in VS Code choose the `oreilly-book` venv as the python interpreter path
