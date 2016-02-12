import urllib2
from walrus import Database

db = Database()
autocomplete = db.autocomplete(namespace='stocks')

def load_data():
    url = 'http://media.charlesleifer.com/blog/downloads/misc/NYSE.txt'
    contents = urllib2.urlopen(url).read()
    for row in contents.splitlines()[1:]:
        ticker, company = row.split('\t')
        autocomplete.store(
            ticker,
            company,
            {'ticker': ticker, 'company': company})

def search(p, **kwargs):
    return autocomplete.search(p, **kwargs)

if __name__ == '__main__':
    autocomplete.flush()
    print 'Loading data (may take a few seconds...)'
    load_data()

    print 'Search stock data by typing a partial phrase.'
    print 'Examples: "uni sta", "micro", "food", "auto"'
    print 'Type "q" at any time to quit'

    while 1:
        cmd = raw_input('? ')
        if cmd == 'q':
            break
        results = search(cmd)
        print 'Found %s matches' % len(results)
        for result in results:
            print '%s: %s' % (result['ticker'], result['company'])
