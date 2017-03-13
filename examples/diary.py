#!/usr/bin/env python

from collections import OrderedDict
import datetime
import sys

from walrus import *

database = Database(host='localhost', port=6379, db=0)

class Entry(Model):
    __database__ = database
    __namespace__ = 'diary'

    content = TextField(fts=True)
    timestamp = DateTimeField(default=datetime.datetime.now, index=True)


def menu_loop():
    choice = None
    while choice != 'q':
        for key, value in menu.items():
            print('%s) %s' % (key, value.__doc__))
        choice = raw_input('Action: ').lower().strip()
        if choice in menu:
            menu[choice]()

def add_entry():
    """Add entry"""
    print('Enter your entry. Press ctrl+d when finished.')
    data = sys.stdin.read().strip()
    if data and raw_input('Save entry? [Yn] ') != 'n':
        Entry.create(content=data)
        print('Saved successfully.')

def view_entries(search_query=None):
    """View previous entries"""
    if search_query:
        expr = Entry.content.search(search_query)
    else:
        expr = None

    query = Entry.query(expr, order_by=Entry.timestamp.desc())
    for entry in query:
        timestamp = entry.timestamp.strftime('%A %B %d, %Y %I:%M%p')
        print(timestamp)
        print('=' * len(timestamp))
        print(entry.content)
        print('n) next entry')
        print('d) delete entry')
        print('q) return to main menu')
        choice = raw_input('Choice? (Ndq) ').lower().strip()
        if choice == 'q':
            break
        elif choice == 'd':
            entry.delete()
            print('Entry deleted successfully.')
            break

def search_entries():
    """Search entries"""
    view_entries(raw_input('Search query: '))

menu = OrderedDict([
    ('a', add_entry),
    ('v', view_entries),
    ('s', search_entries),
])

if __name__ == '__main__':
    menu_loop()
