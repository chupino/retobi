from python
run apt-get update
run apt-get install vim
workdir app
copy . .
run pip install -r requirements.txt

