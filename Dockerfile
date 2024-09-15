from python
run apt-get update
run apt-get install vim -y
workdir app
copy . .
run pip install -r requirements.txt

