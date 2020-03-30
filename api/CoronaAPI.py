import flask

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<h1>Corona API</h1><p>This site list API for Corona</p>"


app.run()
