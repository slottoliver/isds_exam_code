from nltk.corpus import stopwords
import string, re


_letters = list(string.ascii_lowercase)
_numbers = [str(i) for i in range(0, 10)]
_banned = [
    "’", "’", "“", "—", "”", "‘", "–", '#', '[', '/', '(', ')', 
    '{', '}', '\\', '[', ']', '|', '@', ',', ';', '+', '-'
]
_banned = ''.join(_banned) + string.punctuation + ''.join(_numbers)
_boilerplate = [
    '  ', 'https', 'http', 'www', '’s', '―', '/', 'playback', 
    'get', 'mr', 'mrs', 'ms', 'dr', 'prof', 'news', 'report', 
    'unsubscribe', 'they', 'must', 'share', 'that', 'view', 'hide', 
    'copy', 'something', 'enlarge', 'reprint', 'read', '_', 'videos', 
    'autoplay', 'watched', 'press', '’ve', 'toggle', 'around', 'the', 
    's.', 'said', 'here©', 'ad', '#', 'andhis', 'click', 'r', 'device', 
    'contributed', 'advertisement', 'the washington', '&', 'follow', 
    'copyright', 'mrs.', 'photo', 'to', 'also', 'times', 'for', 'however', 
    'fox', 'this', 'copyright ©', 'ofs', 'just', 'wait', 'n’t', 'told', 
    'unsupported', 'i', 'caption', 'ms.', '’m', 'paste', '’re', 'replay', 
    'photos', 'mr.', '©', 'skip', 'watch', '2018', 'cut', 'llc', 'more', 
    'post', 'embed', 'blog', 'b.', 'associated', 'permission'
]
_stop_list = set(stopwords.words('english') + _boilerplate + _letters)
_translation_table = dict.fromkeys(map(ord, _banned), ' ')
_translation_table

def clean_text(text_str):
    text_str = text_str.lower()
    text_str = text_str.translate(_translation_table)
    text_str = re.sub(' +', ' ', text_str)
    text_str = ' '.join([word for word in text_str.split() if word not in _stop_list])
    return text_str