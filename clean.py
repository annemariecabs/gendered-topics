#### IMPORTS
import re, os, h5py
import pyarrow.feather as feather
import pandas as pd
import time

#### CONSTANTS
def get_test_dir_name():
    return os.getcwd()

def get_dataset_dir_name():
    return "/Users/annemariecaballero/Princeton/senior thesis/dataset"

def get_metadata_file_name():
    return get_dataset_dir_name() + "/metadata.ftr"

def get_all_page_nums_regex():
    return r'## p\..*\(#[0-9]+\) ##+'

def get_specified_page_nums_regex(any, roman_numerals):
    if any:
        return r'## p\. [0-9ivxlcdmIVXLCDM]+ \(#[0-9]+\) ##+'

    if roman_numerals:
        return r'## p\. [ivxlcdmIVXLCDM]+ \(#[0-9]+\) ##+'
    else:
        return r'## p\. [0-9]+ \(#[0-9]+\) ##+'

def get_without_specified_page_nums_regex():
    return r'## p\. \(#[0-9]+\) ##+'

def get_specified_page_col_name():
    return 'are_page_nums_specified'

# TODO: skip other filtering if in starts
def get_unfilterable_starts():
    dict = {}
    dict['aeu-ark--13960-t9f485p74-1680103206.txt'] = 8 
    dict['aeu-ark--13960-t4jm33197-1680091485.txt'] = 5
    dict['aeu-ark--13960-t40s0z72h-1680102253.txt'] = 10
    dict['aeu-ark--13960-t7dr3fb1h-1680099286.txt'] = 14
    dict['aeu-ark--13960-t4mk72273-1680097366.txt'] = 12
    dict['coo1-ark--13960-t9k36bk4j-1680088397.txt'] = 17
    dict['mcg-ark--13960-t0ns2cp3b-1680100885.txt'] = 4
    dict['mcg-ark--13960-t1zc9k545-1680102691.txt'] = 3
    dict['mcg-ark--13960-t2d80zr46-1680102509.txt'] = 3
    dict['mcg-ark--13960-t2d80zr46-1680102509.txt'] = 3
    dict['mcg-ark--13960-t3807nn61-1680103373.txt'] = 3
    dict['mcg-ark--13960-t3hx3058t-1680100674.txt'] = 4
    dict['mcg-ark--13960-t3pv84p9f-1680102018.txt'] = 3
    dict['mcg-ark--13960-t4cn8rh2p-1680102063.txt'] = 3
    dict['mcg-ark--13960-t4hm7st34-1680067054.txt'] = 1
    dict['mcg-ark--13960-t51g2bg3x-1680097248.txt'] = 3
    dict['mcg-ark--13960-t5k95rc5w-1680101518.txt'] = 2
    dict['mcg-ark--13960-t6157505m-1680097029.txt'] = 2
    dict['mcg-ark--13960-t6f20m685-1680103499.txt'] = 5
    dict['mcg-ark--13960-t6m06p61r-1680067271.txt'] = 2
    dict['mcg-ark--13960-t78s7916v-1680073196.txt'] = 5
    dict['mcg-ark--13960-t7gq9gj2n-1680103146.txt'] = 5
    dict['mcg-ark--13960-t7pp0r83z-1680065275.txt'] = 3
    dict['mcg-ark--13960-t7sn1tw3t-1680100230.txt'] = 2
    dict['mcg-ark--13960-t8kd3jg7t-1680089034.txt'] = 4
    dict['mcg-ark--13960-t8v99sj6v-1680091665.txt'] = 5
    dict['mcg-ark--13960-t9g478b5v-1680102551.txt'] = 2
    dict['mcg-ark--13960-t9h43ch8k-1680097049.txt'] = 3
    dict['pst-000012026098-1680087496.txt'] = 13
    dict['pst-000072254004-1680089600.txt'] = 7
    dict['uc2-ark--13960-t4zg6h96t-1680100317.txt'] = 3
    dict['uc2-ark--13960-t9086j97b-1680100530.txt'] = 9
    dict['uc2-ark--13960-t3dz07g81-1680095669.txt'] = 6
    dict['bc-ark--13960-t0cv83z0n-1680067181.txt'] = 3
    dict['mcg-ark--13960-t1ck0zp2v-1680094556.txt'] = 7
    dict['mcg-ark--13960-t1zd0rd0c-1680103454.txt'] = 2
    dict['mcg-ark--13960-t2d80zr46-1680102509.txt'] = 3
    dict['mcg-ark--13960-t49p5mb4b-1680099062.txt'] = 4
    dict['coo1-ark--13960-t8hd8cv31-1680109875.txt'] = 10
    dict['emu-010001066240-1680118894.txt'] = 2
    dict['hvd-32044088085477-1680066853.txt'] = 15
    dict['hvd-hn24s3-1680072711.txt'] = 9
    dict['hvd-hn2gna-1680118944.txt'] = 4
    dict['mcg-ark--13960-t04x6xt6f-1680102078.txt'] = 5
    dict['mcg-ark--13960-t09w31022-1680100141.txt'] = 2
    dict['mcg-ark--13960-t0zp5pb01-1680102989.txt'] = 5
    dict['mcg-ark--13960-t1dj72s9f-1680102786.txt'] = 2
    dict['mcg-ark--13960-t1jh56h47-1680101970.txt'] = 1
    dict['mcg-ark--13960-t1xd3ft58-1680120353.txt'] = 3
    dict['mcg-ark--13960-t1xd3fz0n-1680088893.txt'] = 5
    dict['mcg-ark--13960-t21c4jn1k-1680100194.txt'] = 5
    dict['mcg-ark--13960-t2b87tp4p-1680101492.txt'] = 4
    dict['mcg-ark--13960-t2n613q7q-1680101505.txt'] = 4
    dict['mcg-ark--13960-t3903s19v-1680091100.txt'] = 5
    dict['mcg-ark--13960-t3904pp5k-1680067248.txt'] = 6
    dict['mcg-ark--13960-t3jx0xt38-1680103156.txt'] = 2
    dict['mcg-ark--13960-t3kw7zq5x-1680072830.txt'] = 7
    dict['mcg-ark--13960-t3mw4301d-1680102728.txt'] = 5
    dict['mcg-ark--13960-t3nw13t0v-1680103334.txt'] = 5
    dict['mcg-ark--13960-t41r8fk4r-1680102796.txt'] = 2
    dict['mcg-ark--13960-t49p4pv0k-1680103535.txt'] = 3
    dict['mcg-ark--13960-t4hm7sp97-1680071698.txt'] = 3
    dict['mcg-ark--13960-t4mk8wh1b-1680111510.txt'] = 2
    dict['mcg-ark--13960-t63509470-1680109459.txt'] = 5
    dict['mcg-ark--13960-t6449jj9m-1680103020-2.txt'] = 8
    dict['mcg-ark--13960-t6738dq8x-1680102576.txt'] = 2
    dict['mcg-ark--13960-t6c27g259-1680110110.txt'] = 5
    dict['mcg-ark--13960-t6pz7rv84-1680066611.txt'] = 3
    dict['mcg-ark--13960-t6vx2xf27-1680103165.txt'] = 2
    dict['mcg-ark--13960-t6xw62j21-1680094083.txt'] = 5
    dict['mcg-ark--13960-t74t87c03-1680112297.txt'] = 5
    dict['mcg-ark--13960-t78s79517-1680089096.txt'] = 2
    dict['mcg-ark--13960-t79s49x94-1680089085.txt'] = 3
    dict['mcg-ark--13960-t7gq9gj15-1680066241.txt'] = 5
    dict['mcg-ark--13960-t7kp9ng1x-1680102033.txt'] = 3
    dict['mcg-ark--13960-t7kp9nk6p-1680100509.txt'] = 5
    dict['mcg-ark--13960-t8qc2kj7n-1680071738.txt'] = 3
    dict['mcg-ark--13960-t8sb6ng4x-1680109997.txt'] = 5
    dict['mcg-ark--13960-t9281zv19-1680065796.txt'] = 5
    dict['mcg-ark--13960-t9282wm07-1680108277.txt'] = 1
    dict['mcg-ark--13960-t9476zd3m-1680111926.txt'] = 5
    dict['mcg-ark--13960-t95732j2b-1680101938.txt'] = 2
    dict['mcg-ark--13960-t9d528x4t-1680102152.txt'] = 2
    dict['mcg-ark--13960-t9v14m50k-1680102125.txt'] = 5
    dict['mdp-39015010816240-1680066761.txt'] = 14
    dict['mdp-39015024076484-1680092413.txt'] = 13
    dict['mdp-39015054194173-1680090170.txt'] = 9
    dict['mdp-39015078576546-1680110100.txt'] = 5
    dict['mdp-39076002225675-1680091497.txt'] = 9
    dict['mdp-39076002595473-1680092830.txt'] = 6
    dict['nyp-33433082293691-1680065792.txt'] = 7
    dict['osu-32435054478433-1680103442.txt'] = 9
    # one below is empty
    dict['mcg-ark--13960-t10p3mt66-1680103431.txt'] = 2
    dict['coo-31924014519007-1680086962.txt'] = 15

    return dict

def get_unfilterable_ends():
    dict = {}
    dict['aeu-ark--13960-t9f485p74-1680103206.txt'] = 13
    dict['aeu-ark--13960-t4jm33197-1680091485.txt'] = 22
    dict['aeu-ark--13960-t40s0z72h-1680102253.txt'] = 22
    dict['aeu-ark--13960-t4mk72273-1680097366.txt'] = 319
    dict['aeu-ark--13960-t7dr3fb1h-1680099286.txt'] = 184
    dict['coo1-ark--13960-t9k36bk4j-1680088397.txt'] = 92
    dict['mcg-ark--13960-t0ns2cp3b-1680100885.txt'] = 18
    dict['mcg-ark--13960-t1zc9k545-1680102691.txt'] = 12
    dict['mcg-ark--13960-t2d80zr46-1680102509.txt'] = 10
    dict['mcg-ark--13960-t3807nn61-1680103373.txt'] = 8
    dict['mcg-ark--13960-t3hx3058t-1680100674.txt'] = 16
    dict['mcg-ark--13960-t3pv84p9f-1680102018.txt'] = 10
    dict['mcg-ark--13960-t4cn8rh2p-1680102063.txt'] = 16
    dict['mcg-ark--13960-t4hm7st34-1680067054.txt'] = 4
    dict['mcg-ark--13960-t51g2bg3x-1680097248.txt'] = 10
    dict['mcg-ark--13960-t5k95rc5w-1680101518.txt'] = 8
    dict['mcg-ark--13960-t6157505m-1680097029.txt'] = 16
    dict['mcg-ark--13960-t6f20m685-1680103499.txt'] = 31
    dict['mcg-ark--13960-t6m06p61r-1680067271.txt'] = 16
    dict['mcg-ark--13960-t78s7916v-1680073196.txt'] = 62
    dict['mcg-ark--13960-t7gq9gj2n-1680103146.txt'] = 29
    dict['mcg-ark--13960-t7pp0r83z-1680065275.txt'] = 20
    dict['mcg-ark--13960-t7sn1tw3t-1680100230.txt'] = 9
    dict['mcg-ark--13960-t8kd3jg7t-1680089034.txt'] = 18
    dict['mcg-ark--13960-t8v99sj6v-1680091665.txt'] = 31
    dict['mcg-ark--13960-t9g478b5v-1680102551.txt'] = 16
    dict['mcg-ark--13960-t9h43ch8k-1680097049.txt'] = 15
    dict['pst-000012026098-1680087496.txt'] = 27
    dict['pst-000072254004-1680089600.txt'] = 20
    dict['uc2-ark--13960-t4zg6h96t-1680100317.txt'] = 10
    dict['uc2-ark--13960-t9086j97b-1680100530.txt'] = 51
    dict['uc2-ark--13960-t3dz07g81-1680095669.txt'] = 15
    dict['bc-ark--13960-t0cv83z0n-1680067181.txt'] = 28
    dict['mcg-ark--13960-t1ck0zp2v-1680094556.txt'] = 38
    dict['mcg-ark--13960-t1zd0rd0c-1680103454.txt'] = 8
    dict['mcg-ark--13960-t2d80zr46-1680102509.txt'] = 10
    dict['mcg-ark--13960-t49p5mb4b-1680099062.txt'] = 26
    dict['coo1-ark--13960-t8hd8cv31-1680109875.txt'] = 61
    dict['emu-010001066240-1680118894.txt'] = 19
    dict['hvd-32044088085477-1680066853.txt'] = 64
    dict['hvd-hn24s3-1680072711.txt'] = 100
    dict['hvd-hn2gna-1680118944.txt'] = 20
    dict['mcg-ark--13960-t04x6xt6f-1680102078.txt'] = 14
    dict['mcg-ark--13960-t09w31022-1680100141.txt'] = 24
    dict['mcg-ark--13960-t0zp5pb01-1680102989.txt'] = 28
    dict['mcg-ark--13960-t1dj72s9f-1680102786.txt'] = 16
    dict['mcg-ark--13960-t1jh56h47-1680101970.txt'] = 8
    dict['mcg-ark--13960-t1xd3ft58-1680120353.txt'] = 16
    dict['mcg-ark--13960-t1xd3fz0n-1680088893.txt'] = 50
    dict['mcg-ark--13960-t21c4jn1k-1680100194.txt'] = 34
    dict['mcg-ark--13960-t2b87tp4p-1680101492.txt'] = 46
    dict['mcg-ark--13960-t2n613q7q-1680101505.txt'] = 36
    dict['mcg-ark--13960-t3903s19v-1680091100.txt'] = 31
    dict['mcg-ark--13960-t3904pp5k-1680067248.txt'] = 31
    dict['mcg-ark--13960-t3jx0xt38-1680103156.txt'] = 16
    dict['mcg-ark--13960-t3kw7zq5x-1680072830.txt'] = 38
    dict['mcg-ark--13960-t3mw4301d-1680102728.txt'] = 18
    dict['mcg-ark--13960-t3nw13t0v-1680103334.txt'] = 29
    dict['mcg-ark--13960-t41r8fk4r-1680102796.txt'] = 16
    dict['mcg-ark--13960-t49p4pv0k-1680103535.txt'] = 15
    dict['mcg-ark--13960-t4hm7sp97-1680071698.txt'] = 16
    dict['mcg-ark--13960-t4mk8wh1b-1680111510.txt'] = 2
    dict['mcg-ark--13960-t63509470-1680109459.txt'] = 26
    dict['mcg-ark--13960-t6449jj9m-1680103020-2.txt'] = 69
    dict['mcg-ark--13960-t6738dq8x-1680102576.txt'] = 15
    dict['mcg-ark--13960-t6c27g259-1680110110.txt'] = 18
    dict['mcg-ark--13960-t6pz7rv84-1680066611.txt'] = 16
    dict['mcg-ark--13960-t6vx2xf27-1680103165.txt'] = 2
    dict['mcg-ark--13960-t6xw62j21-1680094083.txt'] = 15
    dict['mcg-ark--13960-t74t87c03-1680112297.txt'] = 18
    dict['mcg-ark--13960-t78s79517-1680089096.txt'] = 28
    dict['mcg-ark--13960-t79s49x94-1680089085.txt'] = 15
    dict['mcg-ark--13960-t7gq9gj15-1680066241.txt'] = 38
    dict['mcg-ark--13960-t7kp9ng1x-1680102033.txt'] = 10
    dict['mcg-ark--13960-t7kp9nk6p-1680100509.txt'] = 14
    dict['mcg-ark--13960-t8qc2kj7n-1680071738.txt'] = 32
    dict['mcg-ark--13960-t8sb6ng4x-1680109997.txt'] = 31
    dict['mcg-ark--13960-t9281zv19-1680065796.txt'] = 18
    dict['mcg-ark--13960-t9282wm07-1680108277.txt'] = 4
    dict['mcg-ark--13960-t9476zd3m-1680111926.txt'] = 35
    dict['mcg-ark--13960-t95732j2b-1680101938.txt'] = 16
    dict['mcg-ark--13960-t9d528x4t-1680102152.txt'] = 8
    dict['mcg-ark--13960-t9v14m50k-1680102125.txt'] = 34
    dict['mdp-39015010816240-1680066761.txt'] = 75
    dict['mdp-39015024076484-1680092413.txt'] = 127
    dict['mdp-39015054194173-1680090170.txt'] = 127
    dict['mdp-39015078576546-1680110100.txt'] = 28
    dict['mdp-39076002225675-1680091497.txt'] = 104
    dict['mdp-39076002595473-1680092830.txt'] = 75
    dict['nyp-33433082293691-1680065792.txt'] = 108
    dict['osu-32435054478433-1680103442.txt'] = 67
    dict['mcg-ark--13960-t10p3mt66-1680103431.txt'] = 2
    dict['coo-31924014519007-1680086962.txt'] = 192

    return dict

def get_formatted_title_regex(title):
    return '[\s\S\n]{0,20}'.join(re.sub('[^\sA-Z]', r".", title.upper()).split(' ')[:4])

def get_page_1_end_heuristic_regex():
    return r'PUBLICATIONS|NEW AND USEFUL BOOKS|NEW EDITIONS|LIST OF[\s\n]*BOOKS|EDUCATIONAL WORKS|CATALOGUE|Catalogue|Treasury of|SUITABLE FOR PRIZES|BOOKS FO. YOUNG PEOPLE|List of Books|VALUABLE WORKS|NEW AND POPULAR|LIST OF WORKS|Famous*Books|LIBRARY|Literature Series'

#### METHODS

def get_metadata_df():
    with open(get_metadata_file_name(), 'rb') as f:
        metadata_df = feather.read_feather(f)

    return metadata_df

def get_file_path(dir, is_cotsen, file_name):
    cotsen_dir = "COTSEN" if is_cotsen else "NONCOTSEN"

    return dir + "/" + cotsen_dir + "/" + file_name

def create_folder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)

def get_ht_page_from_page_break(page_break):
    tmp = re.search(r'\(#[0-9]+\)', page_break)
    return int(re.sub(r'[#()]+', '', tmp.group()))

def get_first_page_from_unspecified_pages(pages, title):
    first_page_dict = get_unfilterable_starts()

    if file_name in first_page_dict.keys():
        return first_page_dict[file_name]

    formatted_title = get_formatted_title_regex(title)
    title_pages = [index for index, page in enumerate(pages) if re.findall(formatted_title, page)]

    if len(title_pages) == 0:
        return 0
    else:
        return title_pages[0] + 1

def get_last_page_from_unspecified_pages(file_name, pages):
    last_page_dict = get_unfilterable_ends()

    if file_name in last_page_dict.keys():
        return last_page_dict[file_name]

    ends = [index for index, page in enumerate(pages) if re.findall(r'The end\.|THE END|FINIS\.+[\s\n]|THE MORAL', page)]
    other_ends = [index for index, page in enumerate(pages) if re.findall(r'WORKS BY|WORKS FOR|T.Y BOOKS|This book is under no circumstances to be|This Book is Due|NOTES|THE BORROWER WILL BE CHARGED|OLD STYLE SERIES|CHILDREN.* BOOKS|HISTORY BOOKS|LITTLE .* BOOKS|Index|[^A-Z]INDEX', page)]

    both_ends = []

    if len(ends) > 0:
        both_ends.append(ends.pop())

    if len(other_ends) > 0:
        both_ends.append(other_ends[0])

    if len(both_ends) == 2:
        end = max(both_ends[0], both_ends[1])
    elif len(both_ends) == 1:
        end = both_ends[0]
    else:
        end = len(pages)
    
    return end

def get_chapters_and_scenes(pages):
    scene_chapter_regex = r'(?:scene\s|chapter\s|chap.\s|volume|vol.\s)'
    # based on modern roman numerals, flexible: 
    # #https://www.oreilly.com/library/view/regular-expressions-cookbook/9780596802837/ch06s09.html
    roman_numerals_regex = r'(?=[MDCLXVI])M*(?:C[MD]|D?C*)(?:X[CL]|L?X*)(?:I[XV]|V?I*)'
    numbers_regex = r'[0-9]+'

    regex = scene_chapter_regex + r'(?:' + roman_numerals_regex + r'|' + numbers_regex + r')[\.:,]?\s' + r"(?:[A-Z][A-Z,']*(?: [A-Z&][A-Z,']*)*[:\.]?\n)?"

    all_page_titles = [re.findall(regex, page, re.IGNORECASE)
        for page in pages]
    
    return [item.strip() for sublist in all_page_titles for item in sublist]

def get_all_caps_headers_as_set(pages):
    pages = [page for page in pages if page.strip()]

    tmp_page_lines = [re.findall(r"[A-Z][A-Z,']*(?:\s[A-Z&][A-Z,']*)*[:\.]?", page) 
        for page in pages]
    
    all_page_lines = []
    
    for page_lines in tmp_page_lines:
        all_page_lines.append([x for x in page_lines if not re.fullmatch(r"[A-Z][',.]?", x)])

    repeated = set()

    for i, page_lines in enumerate(all_page_lines):
        if i == 0:
            continue

        for elem in set(all_page_lines[i - 1]).intersection(all_page_lines[i]):
            repeated.add(elem)

        if i < 2: 
            continue 

        # account for off-page phenomena
        for elem in set(all_page_lines[i - 2]).intersection(all_page_lines[i]):
            repeated.add(elem)
    
    return repeated

def get_any_headers_as_set(pages):
    # get rid of empty pages
    pages = [page for page in pages if page.strip()]

    repeated = set()

    all_page_lines = [page.splitlines()
        for page in pages]

    for i, page_lines in enumerate(all_page_lines):
        if i < 1:
            continue

        for elem in set(all_page_lines[i - 1]).intersection(all_page_lines[i]):
            repeated.add(elem)

        if i < 2: 
            continue 

        # account for off-page phenomena
        for elem in set(all_page_lines[i - 2]).intersection(all_page_lines[i]):
            repeated.add(elem)

    repeated = [repeat for repeat in repeated if re.search(r"[A-Z][A-Za-z,']*(?:\s[A-Z&][A-Za-z,']*)*[:\.]?", repeat) and repeat != "I"]

    return repeated 

def not_mix(matchobj):
    if 'mix' in matchobj.group() or 'MIX' in matchobj.group(): 
        return matchobj.group()
    else: 
        return ''

# including page numbers - simply don't care about numbers tbh
def remove_numbers_and_numerals(pages):
    roman_numerals_regex = r'[^A-Z](?=[MDCLXVI]{2,})M*(?:C[MD]|D?C*)(?:X[CL]|L?X*)(?:I[XV]|V?I*)[^A-Z]'
    numbers_regex = r'[0-9]+[^snrt]'

    # because mix is a real word
    return [re.sub(roman_numerals_regex + r'|' + numbers_regex, not_mix, page, flags=re.IGNORECASE) 
        for page in pages]

def remove_contents_and_illustrations_list(pages):
    return [page for page in pages if re.search(r'(?:CONTENTS|Contents|ILLUSTRATIONS|Illustrations|FRONTISP.*)\.?', page) is None]

def convert_to_pages_with_unspecified_page_nums(text, file_name, title):
    page_break_delimiter = '<PAGE_BREAK>'

    replaced_text = re.sub(get_all_page_nums_regex(), page_break_delimiter, text)
    pages = replaced_text.split(page_break_delimiter)

    # cut off the page that hathitrust adds
    if not file_name in get_unfilterable_starts().keys():
        pages = pages[1:]

    first_page = get_first_page_from_unspecified_pages(pages, title)
    last_page = get_last_page_from_unspecified_pages(file_name, pages)

    if last_page < len(pages):
        return pages[first_page:last_page]
    else:
        return pages[first_page:]

def convert_to_pages_with_specified_page_nums(text, file_name):
    page_break_delimiter = '<PAGE_BREAK>'
    roman_numerals_page_break_delimiter = '<ROMAN_NUM_PAGE_BREAK>'

    file_start = file_name.split('-')[0]
    # always cut off first page because that's the information that hathitrust provides
    first_page_index = 1
    page_1 = re.findall(r'## p. 1 \(#[0-9]+\) ##+', text)

    replaced_text = text

    if 'uc2-ark--13960-t1mg7h76c' not in file_name: 
        replaced_text = re.sub(get_specified_page_nums_regex(False, True), page_break_delimiter + roman_numerals_page_break_delimiter, text)
    
    replaced_text = re.sub(get_all_page_nums_regex(), page_break_delimiter, replaced_text)
    pages = replaced_text.split(page_break_delimiter)

    spec_page_breaks = re.findall(get_specified_page_nums_regex(False, False), text)

    first_page_break = spec_page_breaks[0]
    last_page_break = spec_page_breaks[len(spec_page_breaks) - 1]

    if last_page_break is not None: 
        last_page_index = get_ht_page_from_page_break(last_page_break)
    else:
        print("CAUTION: Extremely short file - investigate!")

    if first_page_break is not None:
        first_page_index = get_ht_page_from_page_break(first_page_break)

        # weird quirk where uc2 files tend to have specified numbering start a page too late
        if file_start == 'uc2' and first_page_index != 0:
            first_page_index -= 1

        if len(page_1) > 0:
            page_1_num = get_ht_page_from_page_break(page_1[0])
            if page_1_num > first_page_index:
                if re.search(get_page_1_end_heuristic_regex(), pages[page_1_num]) is not None:
                    last_page_index = page_1_num - 1
            page_1_num = get_ht_page_from_page_break(page_1[len(page_1) - 1])
    else: 
        print('CAUTION: File that should have specified page numbers does not.')

    ### TWO STRANGE EXCEPTIONS: noticed in manual inspection
    if file_start == 'wau':
        last_page_index = 29

    # because of special start, iau will have roman numerals and that's fine
    if 'iau-31858014734440' in file_name:
        first_page_index = 15
    
    pages = pages[first_page_index:last_page_index + 1]

    final_pages = pages

    # cleaning just wau of the weird in between pages
    if file_start == "wau" or file_start == 'umn':
        final_pages = []

        for i, page in enumerate(pages):
            if roman_numerals_page_break_delimiter in page:
                continue

            final_pages.append(page)

    return final_pages 

# credit: https://aaltoscicomp.github.io/python-for-scicomp/data-formats/
def save_pages_file(pages, dir_name, file_name):
    h5_file = h5py.File(dir_name + '/' + file_name + '.h5', 'w')
    # Write dataset
    h5_file.create_dataset('data_array', data=pages)
    # Close file and write data to disk. Important!
    h5_file.close()
    
def remove_line_interrupts(text):
    return re.sub(r'-\s+', '', text)

def get_cleaned_pages(pages):    
    new_pages = []
    for i, page in enumerate(pages):
        new_page = remove_line_interrupts(page)
        if len(new_page) > 1:
            new_pages.append(new_page)
    
    return new_pages

### CODE


# create the cleaned directory
clean_dir = get_dataset_dir_name() + "/cleaned"
create_folder(clean_dir)

# load the metadata as metadata_df
metadata_df  = get_metadata_df()

# add the column and values for specified page numbers - only needs to happen once but could delete the column and rerun if necessary
if get_specified_page_col_name() not in metadata_df.columns:
    print('Create the specified page column')
    metadata_df.insert(len(metadata_df.columns), get_specified_page_col_name(), False)

    dataset_dir = get_dataset_dir_name()

    for index, row in metadata_df.iterrows():
        file_name = row['file_name']
        is_cotsen = row['cotsen']

        text = ''

        with open(get_file_path(dataset_dir, is_cotsen, file_name), 'utf-8') as input:
            text = input.read()
        
        # look for the appearance of any specified pages
        spec_pages = re.search(get_specified_page_nums_regex(any=True, roman_numerals=False), text)

        if spec_pages is not None:
            metadata_df.at[index, get_specified_page_col_name()] = True

metadata_df.to_feather(get_metadata_file_name())


# iterate through, separate, and clean all of the .txt files
for index, row in metadata_df.iterrows():
    start = time.time()

    file_name = row['file_name']
    is_cotsen = row['cotsen']
    title = row['title']
    name = file_name.split('.txt')[0]

    print('-----------------------------------------------\nProcessing ' + file_name)

    if (os.path.exists(clean_dir + '/' + name + '/done.txt')):
        print('Skipped ' + file_name + ' because it has already been processed')
        continue 

    file_path = get_file_path(get_dataset_dir_name(), is_cotsen, file_name)

    text = ''
    
    with open(file_path) as input:
        text = input.read()

    print('Read pages in ' + str(time.time() - start))

    text = remove_line_interrupts(text)

    has_spec_pages = row[get_specified_page_col_name()]

    if has_spec_pages:
        pages = convert_to_pages_with_specified_page_nums(text, file_name)
    else: 
        pages = convert_to_pages_with_unspecified_page_nums(text, file_name, title)
    
    print('Split pages in ' + str(time.time() - start))

    clean_pages = get_cleaned_pages(pages)

    clean_pages_without_numbers = remove_numbers_and_numerals(pages)

    caps_headers = get_all_caps_headers_as_set(clean_pages_without_numbers)
    pages_without_caps_headers = [re.sub(re.escape(r'|'.join(caps_headers)), '', page)
                for page in clean_pages]
    
    any_headers = get_any_headers_as_set(pages)

    
    pages_without_any_headers = [re.sub(re.escape(r'|'.join(any_headers)), '', page)
                for page in pages_without_caps_headers]
    
    final_pages = remove_contents_and_illustrations_list(pages_without_any_headers)

    almost_final_text = '\n'.join(final_pages)
    final_text = ''.join([s for s in almost_final_text.splitlines() if s.strip()])

    new_file_dir = clean_dir + '/' + name

    print('Cleaned pages in ' + str(time.time() - start))

    create_folder(new_file_dir)

    with open(new_file_dir + "/" + file_name, "w") as f:
        f.write(final_text)

    save_pages_file(pages, new_file_dir, name + '_pages')

    with open(new_file_dir + "/done.txt", "w") as f:
        f.write('')

    print('Saved pages and finished processing ' + file_name + ' in ' + str(time.time() - start))

print('YOU\'RE DONE HURRAY!!!')
