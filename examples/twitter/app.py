import datetime
import operator

from flask import abort
from flask import flash
from flask import Flask
from flask import redirect
from flask import render_template
from flask import request
from flask import session
from flask import url_for
from functools import wraps
from hashlib import md5
from walrus import *

# Configure our app's settings.
DEBUG = True
SECRET_KEY = 'hin6bab8ge25*r=x&amp;+5$0kn=-#log$pt^#@vrqjld!^2ci@g*b'

# Create a flask application - this `app` object will be used to handle
# inbound requests, routing them to the proper 'view' functions, etc.
app = Flask(__name__)
app.config.from_object(__name__)

# Create a walrus database instance - our models will use this database to
# persist information.
database = Database()

# Model definitions - the standard "pattern" is to define a base model class
# that specifies which database to use. Then, any subclasses will automatically
# use the correct storage.
class BaseModel(Model):
    __database__ = database
    __namespace__ = 'twitter'

# Model classes specify fields declaratively, like django.
class User(BaseModel):
    username = TextField(primary_key=True)
    password = TextField(index=True)
    email = TextField()

    followers = ZSetField()
    following = ZSetField()

    def get_followers(self):
        # Because all users are added to the `followers` sorted-set with the
        # same score, when retrieved they will be sorted by key (username).
        return [User.load(username) for username in self.followers]

    def get_following(self):
        # Because all users are added to the `following` sorted-set with the
        # same score, when retrieved they will be sorted by key (username).
        return [User.load(username) for username in self.following]

    def is_following(self, user):
        # We can use Pythonic operators when working with Walrus containers.
        return user.username in self.following

    def gravatar_url(self, size=80):
        return 'http://www.gravatar.com/avatar/%s?d=identicon&s=%d' % \
            (md5(self.email.strip().lower().encode('utf-8')).hexdigest(), size)


# Simple model with a one-to-many relationship: one user has 0..n messages.
# A user is associated with a message via the `username` field.
class Message(BaseModel):
    username = TextField(index=True)
    content = TextField()
    timestamp = DateTimeField(default=datetime.datetime.now)

    def get_user(self):
        return User.load(self.username)


# Flask provides a `session` object, which allows us to store information
# across requests (stored by default in a secure cookie). This function allows
# us to mark a user as being logged-in by setting some values in the session:
def auth_user(user):
    session['logged_in'] = True
    session['username'] = user.username
    flash('You are logged in as %s' % (user.username))

# Get the currently logged-in user, or return `None`.
def get_current_user():
    if session.get('logged_in'):
        try:
            return User.load(session['username'])
        except KeyError:
            session.pop('logged_in')

# View decorator which indicates that the requesting user must be authenticated
# before they can access the wrapped view. The decorator checks the session to
# see if they're logged in, and if not redirects them to the login view.
def login_required(f):
    @wraps(f)
    def inner(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return inner

# Retrieve an object by primary key. If the object does not exist, return a
# 404 not found.
def get_object_or_404(model, pk):
    try:
        return model.load(pk)
    except ValueError:
        abort(404)

# Custom template filter: Flask allows you to define these functions and then
# they are accessible in the template. This one returns a boolean whether the
# given user is following another user.
@app.template_filter('is_following')
def is_following(from_user, to_user):
    return from_user.is_following(to_user)

# Views: these are the actual mappings of url to view function.
@app.route('/')
def homepage():
    # Depending on whether the requesting user is logged in or not, show them
    # either the public timeline or their own private timeline.
    if session.get('logged_in'):
        return private_timeline()
    else:
        return public_timeline()

@app.route('/private/')
def private_timeline():
    # The private timeline is a bit interesting as it shows how to create a
    # query dynamically. We are taking all the users the current user follows
    # and basically performing a big set union on message objects. Matching
    # messages are then sorted newest to oldest.
    user = get_current_user()
    if user.following:
        query = reduce(operator.or_, [
            Message.username == username
            for username, _ in user.following
        ])
        messages = Message.query(query, order_by=Message.timestamp.desc())
    else:
        messages = []
    return render_template('private_messages.html', message_list=messages)

@app.route('/public/')
def public_timeline():
    # Display all messages, newest to oldest.
    messages = Message.query(order_by=Message.timestamp.desc())
    return render_template('public_messages.html', message_list=messages)

@app.route('/join/', methods=['GET', 'POST'])
def join():
    if request.method == 'POST' and request.form['username']:
        username = request.form['username']
        try:
            User.load(username)
        except KeyError:
            user = User.create(
                username=username,
                password=md5(request.form['password']).hexdigest(),
                email=request.form['email'])
            auth_user(user)
            return redirect(url_for('homepage'))
        else:
            flash('That username is already taken')

    return render_template('join.html')

@app.route('/login/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST' and request.form['username']:
        try:
            user = User.get(
                (User.username == request.form['username']) &
                (User.password == md5(request.form['password']).hexdigest()))
        except ValueError:
            flash('The password entered is incorrect')
        else:
            auth_user(user)
            return redirect(url_for('homepage'))

    return render_template('login.html')

@app.route('/logout/')
def logout():
    session.pop('logged_in', None)
    flash('You were logged out')
    return redirect(url_for('homepage'))

@app.route('/following/')
@login_required
def following():
    # Get the list of user objects the current user is following.
    user = get_current_user()
    return render_template('user_following', user_list=user.get_following())

@app.route('/followers/')
@login_required
def followers():
    # Get the list of user objects the current user is followed by.
    user = get_current_user()
    return render_template('user_following', user_list=user.get_followers())

@app.route('/users/')
def user_list():
    # Display all users ordered by their username.
    users = User.query(order_by=User.username)
    return render_template('user_list.html', user_list=users)

@app.route('/users/<username>/')
def user_detail(username):
    # Using the "get_object_or_404" shortcut here to get a user with a valid
    # username or short-circuit and display a 404 if no user exists in the db.
    user = get_object_or_404(User, username)

    # Get all the users messages ordered newest-first.
    messages = Message.query(
        Message.username == user.username,
        order_by=Message.timestamp.desc())
    return render_template(
        'user_detail.html',
        message_list=messages,
        user=user)

@app.route('/users/<username>/follow/', methods=['POST'])
@login_required
def user_follow(username):
    current_user = get_current_user()
    user = get_object_or_404(User, username)
    current_user.following.add(user.username, 0)
    user.followers.add(current_user.username, 0)

    flash('You are following %s' % user.username)
    return redirect(url_for('user_detail', username=user.username))

@app.route('/users/<username>/unfollow/', methods=['POST'])
@login_required
def user_unfollow(username):
    current_user = get_current_user()
    user = get_object_or_404(User, username)
    current_user.following.remove(user.username)
    user.followers.remove(current_user.username)

    flash('You are no longer following %s' % user.username)
    return redirect(url_for('user_detail', username=user.username))

@app.route('/create/', methods=['GET', 'POST'])
@login_required
def create():
    # Create a new message.
    user = get_current_user()
    if request.method == 'POST' and request.form['content']:
        message = Message.create(
            username=user.username,
            content=request.form['content'])
        flash('Your message has been created')
        return redirect(url_for('user_detail', username=user.username))

    return render_template('create.html')

@app.context_processor
def _inject_user():
    return {'current_user': get_current_user()}

# allow running from the command line
if __name__ == '__main__':
    app.run()
