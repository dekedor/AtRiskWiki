import urbandict
import pandas as pd
import re

def BadWords(subject):
    defs = urbandict.define(subject)
    numDefs = len(defs)
    badWords = \
    set(pd.read_csv('full-list-of-bad-words-banned-by-google-txt-file_2013_11_26_04_53_31_867.txt')['Word'].values)
    numBadDefs = \
    sum([1 if set(re.split(' ', re.sub('\n', '', d['def'].lower()))).intersection(badWords) else 0 for d in defs])
    return numDefs, numBadDefs
