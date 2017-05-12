import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint
'''
Wrangling Open Street Map Data for -
Vancouver, Canada

In This Script to get sense of Data ->

1. Count the number of tags for each type of Tag
3. Create csv from the XML
'''
#OSM File Name and path
OSM_PATH = 'vancouver_canada.osm'
SAMPLE = 'sample_vc.osm'
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
#Regex for different cases
#Finding counts for different tag categories
def get_element(osm_file, tags=('node', 'way', 'relation')):

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end':
            yield elem
            root.clear()

def key_type(element, keys):
    '''
        Count of validated strings in different tags
    '''
    if element.tag == "tag":
        for tag in element.iter('tag'):
            k = tag.get('k')
            if lower.search(k):
                keys['lower'] += 1
            elif lower_colon.search(k):
                keys['lower_colon'] += 1
            elif problemchars.search(k):
                keys['problemchars'] += 1
            else:
                keys['other'] += 1
    return keys

def process_map(filename):
    '''
    Function for Distributing in different categories and finding tag count
    '''
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys

def count_tags(filename):
    '''
    Initial Function to get a sense of data, how is the data structured
    .
    '''
    tags = {}
    for element in get_element(filename):
        if element.tag not in tags.keys():
            tags[element.tag] = 1
        else:
            tags[element.tag] += 1
    return tags
def test(filename):
    keys = process_map(filename)
    tags = count_tags(filename)
    pprint.pprint(keys)
    pprint.pprint(tags)


if __name__ == "__main__":
    #test(OSM_PATH)
    test(SAMPLE)
