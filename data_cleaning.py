import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pickle
import cerberus
import schema
import codecs
import csv
'''
Wrangling Open Street Map Data for -
Vancouver, Canada

In This Script as we already have a sense of data ->

1. Audit Specific Attributes
2. Update the Data Attributes, If Required
3. Create csv from the XML
'''
#OSM File Name and path
OSM_PATH = 'vancouver_canada.osm'
SAMPLE = 'sample_vc.osm'

#CSV Files (Where we'll be writing cleaned data)
NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"
#Column Tags-Fields
SCHEMA = schema.schema

NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

#These expressions are used for validating strings in a particular format.

contains_colon = re.compile(r'([a-z]|_)+:([a-z]|_)+:*\S*')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

#Initial expected street names
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons"]

# Mapping of Abbreviated Street Names to their full form
street_mapping={  'Ave'  : 'Avenue',
           'Ave.' : 'Avenue',
           'Apt'  : 'Apartment',
           'Blvd' : 'Boulevard',
           'Dr.'   : 'Drive',
           'Dr'    : 'Drive',
           'Ln'   : 'Lane',
           'Pkwy' : 'Parkway',
           'Rd'   : 'Road',
           'Rd.'  : 'Road',
           'St'   : 'Street',
           'St.'   : 'Street',
           'street' :"Street",
           'Ct'   : "Court",
           'Cir'  : "Circle",
           'Cr'   : "Court",
           'ave'  : 'Avenue',
           'Hwg'  : 'Highway',
           'Hwy'  : 'Highway',
           'Sq'   : "Square",
           '420'  : "420",
           'Ext'  : "Extension",
        }
# For Correcting State Name
state_mapping = {
    'BC' :'British Columbia'
}

'''
Helper Functions to Process the OSM(XML) elements
'''
def get_element(osm_file, tags=('node', 'way', 'relation')):

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end':
            yield elem
            root.clear()
def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.items())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)

        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.items()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


'''
Audit and Update Functions for Correcting the Data In XML
'''

def is_street_name(elem):
    '''
        Determine wether an element is actually a street name
    '''
    # determine whether a element is a street name
    return (elem == "addr:street")

def audit_street_type(street_name):
    '''
    Function to add unexpected street name to a list (Steeet_types)
    '''
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            return False
        else:
            return True


def update_street_name(name, mapping, regex):
    '''
    Function to update steet name to correct format.
    '''
    m = regex.search(name)
    if m:
        street_type = m.group()
        if street_type in mapping:
            name = re.sub(regex, mapping[street_type], name)     #replacing abbreviation with mapped value

    return name

def is_postcode(elem):
    # Function to check if the element is a postcode
    return (elem == "addr:postcode")

def audit_update_postcode(postcode):
    '''
    Function to validate post code and adding it to invalid if not satisfying the conditions
    '''
    if not re.match(r'^\S{3}\s\S{3}$', postcode): #Match the pattern of post code
        return "".join(postcode.split())
    else:
        return postcode


def is_phonenum(elem):
    return (elem == "phone")

def audit_update_phone(phone):
    # Function to Check if phone number is valid if not, update it
    if phone[:2] == '+1':
      return phone[:2]+''.join(e for e in phone[3:] if e.isalnum())
    else:
      return '+1'+''.join(e for e in phone if e.isalnum())

def is_housenumber(elem):
    return (elem == 'addr:housenumber')

def audit_update_house(house):
    # Function to check if the house number is valid, if not update it.
    if house[:1] == '#':
      return house[1:]
    else:
      return house

def is_province(elem):
    return (elem == 'is_in')

def audit_udpate_province(prov):
    #Function to check if the province is tagged as 'BC' instead of full 'British Columbia'. Fixing if its incorrect
    if prov[-4:] == ', BC':
        return prov[0:-4]+', British Columbia'
    else:
        return prov
'''
Functions to get Tag Fields and call the audit functions for validation and updation
'''
def split_key_type(m):
    m = re.split(":", m.group())
    tag_type = m[0]
    if len(m) == 2:
        key = m[1]
    else:
        key = str(m[1] + ":"+ m[2])
    return key, tag_type


def get_node_way_att(element, attribs, FIELDS):
    '''
    This function is to get the attribute values of the tags for 'node' and 'way'.
    In the dataset we have some data points may not have all the keys i.e not all data points have 'user','id' etc
    so for that, handling the scenario here, if key doesn't exist. giving a default value '-999'
    '''
    if element.tag == "node":
        for tag in element.iter("node"):
            for att in FIELDS:
                x = tag.get(att,None)
                if x is None:
                    attribs[att] = '-999'
                else:
                    attribs[att] = tag.attrib[att]
    elif element.tag == "way":
        for tag in element.iter("way"):
            for att in FIELDS:
                x = tag.get(att,None)
                if x is None:
                    attribs[att] = '-999'
                else:
                    attribs[att] = tag.attrib[att]
    return attribs


def get_way_node(element, child, way_nodes, pos):
    way_temp = {}
    way_temp['id']= element.attrib['id']
    way_temp['node_id'] = child.attrib['ref']
    way_temp['position'] = pos
    way_nodes.append(way_temp)
    return way_nodes

def get_tag_fields(element, child, tags, default_tag_type):
    tag_temp = {}
    new_tags = tags
    tag_temp['id'] = element.attrib['id']
    key = child.attrib['k']
    if PROBLEMCHARS.search(key):
        return None
    m = contains_colon.search(key)
    if m:
        tag_temp['key'], tag_temp['type'] = split_key_type(m)
    else:
        tag_temp['type'] = default_tag_type
        tag_temp['key'] = key

    value = child.attrib['v']
    #Validating the data points.
    if is_street_name(key):
        if audit_street_type(value):
            value = update_street_name(value,street_mapping,street_type_re)
    elif is_postcode(key):
        value = audit_update_postcode(value)
    elif is_phonenum(key):
        value = audit_update_phone(value)
    elif is_province(key):
        value = audit_udpate_province(value)
    elif is_housenumber(key):
        value = audit_update_house(value)

    tag_temp['value'] = value

    new_tags.append(tag_temp)
    return new_tags

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""
    attribs = {}
    way_nodes = []
    tags = []
    for i, child in enumerate(element):
        if child.tag =='tag':
            tags = get_tag_fields(element, child, tags, default_tag_type)
        # only way nodes will have a child tag of 'nd' but this saves processing time this way
        elif child.tag == 'nd':
            way_nodes = get_way_node(element, child, way_nodes, i)

    if element.tag == 'node':
        node_attribs = get_node_way_att(element, attribs, NODE_FIELDS)
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        way_attribs = get_node_way_att(element, attribs, WAY_FIELDS)
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}
'''
Main Function - Process Map
'''
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
        codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
        codecs.open(WAYS_PATH, 'w') as ways_file, \
        codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
        codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])
if __name__ == "__main__":
    process_map(SAMPLE, validate=False)
