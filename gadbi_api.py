# all the imports

from flask import Flask, render_template
from backend import read_file_from_hdfs, read_absolute_values

# configuration
DEBUG = True
USERNAME = 'guilhermemg'
WEB_PORT=80
WEB_HOST="localhost"


# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)

app.config.from_envvar('FLASKR_SETTINGS', silent=True)

@app.route('/')
def show_options():
	entries = read_absolute_values()
	return render_template('index.html', entries=entries)

@app.route('/idioms', methods=['GET'])
def show_idioms():
	read_file_from_hdfs('idioms')
	return render_template('idioms.html')
		
@app.route('/pages', methods=['GET'])
def show_pages():
	read_file_from_hdfs('pages')
	return render_template('pages.html')

@app.route('/editors', methods=['GET'])
def show_editors():
	read_file_from_hdfs('editors')
	return render_template('editors.html')


if __name__ == '__main__':
	app.run(host=WEB_HOST, port=WEB_PORT, debug=app.config.get('DEBUG', DEBUG))
