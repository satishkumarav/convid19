import flask
from flask import jsonify, abort, make_response
from flask_restful import Api,Resource
from flask_httpauth import HTTPBasicAuth

app = flask.Flask(__name__)
app.config["DEBUG"] = True

locations = [
    {
        'location': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'location': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]

auth = HTTPBasicAuth()



@auth.get_password
def get_password(username):
    if username == 'merlin':
        return 'arthur'
    else:
        return None


@auth.error_handler
def unauthorized():
    return make_response(jsonify({'error': 'Unauthorized access'}), 401)





@app.route('/', methods=['GET'])
def home():
    return "<h1>Corona DB API</h1><p>This site list API for Corona</p>"


@app.route('/corona/api/v1.0/locations', methods=['GET'])
@auth.login_required
def getAllLocations():
    return jsonify({'locations': locations})


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/corona/api/v1.0/locations/<int:location_id>', methods=['GET'])
@auth.login_required
def getLocations(location_id):
    location = [location for location in locations if location['location'] == location_id]
    if (len(location)) == 0:
        abort(404)
    return jsonify({'task': location[0]})


# @app.route('/hello/<name>')
# def hello_world(name):
#     return 'Hello %s!' % name
#
#
# @app.route('/blog/<int:postID>')
# def show_blog(postID):
#     return 'Blog Number %d' % postID
#
#
# @app.route('/rev/<float:revNo>')
# def revision(revNo):
#     return 'Revision Number %f' % revNo


# @app.route('/flask')
# def hello_flask():
#     return 'Hello Flask'
#
#
# @app.route('/python/')
# def hello_python():
#     return 'Hello Python'
#
#
# @app.route('/admin')
# def hello_admin():
#     return 'Hello Admin'
#
#
# @app.route('/guest/<guest>')
# def hello_guest(guest):
#     return 'Hello %s as Guest' % guest
#
#
# @app.route('/user/<name>')
# def hello_user(name):
#     if name == 'admin':
#         return redirect(url_for('hello_admin'))
#     else:
#         return redirect(url_for('hello_guest', guest=name))
#
#
# @app.route('/success/<name>')
# def success(name):
#     return 'welcome %s' % name
#
#
# @app.route('/login', methods=['POST', 'GET'])
# def login():
#     if request.method == 'POST':
#         user = request.form['nm']
#         return redirect(url_for('success', name=user))
#     else:
#         user = request.args.get('nm')
#         return redirect(url_for('success', name=user))


if __name__ == '__main__':
    app.run(debug=True)
