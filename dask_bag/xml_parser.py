#!/usr/bin/env python

import xmltodict
import json
import argparse
import sys

class XMLParser:

    def __init__(self, input_xml, output_json=None):
        self.input_xml = input_xml
        self.output_json = output_json
        self.initialize_properties()

    def initialize_properties(self):
        self._records = None
        self._parse = None

    def xml_file_to_dict(self):
        xml_file = open(self.input_xml, "rb")
        data_dict = xmltodict.parse(xml_file,
                                    dict_constructor=dict)
        xml_file.close()
        return data_dict

    @property
    def parse(self):
        if self._parse:
            return self._parse

        variety = self.input_xml[-5:-4]
        if variety == 'T':
            self._parse = self.parse_theaters_data
        elif variety == 'S':
            self._parse = self.parse_showings_data
        elif variety == 'I':
            self._parse = self.parse_movies_data
        else:
            raise ValueError('Parser type unknown')

        return self._parse

    @property
    def records(self):
        if self._records:
            return self._records

        self._records = self.parse()

        return self._records

    def parse_general_data(self, key1, key2):
        data_dict = self.xml_file_to_dict()
        temp = data_dict.get(key1)

        if not temp:
            return []

        temp = temp.get(key2)
        if not temp:
            return []

        if type(temp) != list:
            temp = [temp]

        country = self.get_country()
        for t in temp:
            t['country'] = country
            t['source_xml'] = self.input_xml
        return temp

    def get_country(self):
        split = self.input_xml.split('/')
        return split[-2]

    def parse_showings_data(self):
        return self.parse_general_data('times', 'showtime')

    def parse_movies_data(self):
        return self.parse_general_data('movies', 'movie')

    def parse_theaters_data(self):
        return self.parse_general_data('houses', 'theater')

    def write(self):
        json_file = sys.stdout
        if self.output_json:
            json_file = open(self.output_json, "w")

        for record in self.records:
            json_file.write(json.dumps(record) + "\n")

        if self.output_json:
            json_file.close()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('-o', '--output')
    args = parser.parse_args()

    XMLParser(args.input, args.output).write()
