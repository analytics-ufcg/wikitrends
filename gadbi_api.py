# all the imports

from flask import Flask, render_template
from backend import read_file_from_hdfs, read_absolute_values

# configuration
DEBUG = True
USERNAME = 'ubuntu'
WEB_PORT=80
WEB_HOST="localhost"


# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)

app.config.from_envvar('FLASKR_SETTINGS', silent=True)

entries = read_absolute_values()

@app.route('/')
def show_options():
	return render_template('index.html')

@app.route('/idioms', methods=['GET'])
def show_idioms():
	entries = read_absolute_values()
	read_file_from_hdfs('idioms')
	return render_template('idioms.html', entries=entries)
		
@app.route('/pages', methods=['GET'])
def show_pages():
	entries = read_absolute_values()
	read_file_from_hdfs('pages')
	return render_template('pages.html', entries=entries)

@app.route('/editors', methods=['GET'])
def show_editors():
	entries = read_absolute_values()
	read_file_from_hdfs('editors')
	return render_template('editors.html', entries=entries)


if __name__ == '__main__':
	app.run(host=WEB_HOST, port=WEB_PORT, debug=app.config.get('DEBUG', DEBUG))
