import csv
import nltk
from nltk.metrics import *
import json
import math
from time import time, sleep

import numpy as np

import random

import sqlite3

# import mysql

# mysql.connect

con = sqlite3.connect('patientsDB.db')
cur = con.cursor()

with open('./state_abbreviations.json', 'r') as abbr_file:
    abbreviations = json.load(abbr_file)

# returns if string is not empty if string
# returns if object not None otherwise?
def row_is_good(row):
    return row[0] and row[1]

# returns levenshtein distance or -1
def get_distance_between_entries(row, distancethreshold=-1):
    if row_is_good(row):
        # get string distance between two numbers
        row_distance = edit_distance(*row, True)
        return row_distance
    else:
        return -1

# returns True if entries in row have same first letter
def is_first_letter_in_row_equal(row):
    return row[0][0] == row[1][0]

def handle_names(names, score_decrement=5):
    # if difference in length is more than 2
    name_0_len = len(names[0])
    name_1_len = len(names[1])

    finished = False

    # diff in length is more than 2
    if abs(name_0_len - name_1_len) > 2:
        # see if one of them is short for the other
        if name_0_len < 4 and name_0_len < name_1_len:
            # if it DOES start with fn0
            if names[1].startswith(names[0]):
                return -score_decrement
                finished = True
        elif name_1_len < 4 and name_1_len < name_0_len:
            # if it DOES start with fn1
            if names[0].startswith(names[1]):
                return -score_decrement
                finished = True
    # if one is not short for another
    if not finished:
        # add levenshtein distance weighted; squared?
        names_distance = edit_distance(*names, True)
        # divide by 2 to keep the affect on score a little lower
        # otherwise this term would overpower
        return math.pow(names_distance, 2) / 2

def handle_zip_codes(zip_codes, score_decrement=5):
    if get_distance_between_entries(zip_codes) > 1:
        return 15
    else:
        return -score_decrement

def handle_streets_1(streets, score_decrement=5):
    if get_distance_between_entries(streets) > 3:
        return 15
    else:
        return -score_decrement

def handle_streets_2(streets, score_decrement=5):
    # if the numbers are the same, subtract
    numbers_0 = ""
    numbers_1 = ""
    for letter in streets[0]:
        if letter.isdigit():
            numbers_0 += letter
    for letter in streets[1]:
        if letter.isdigit():
            numbers_1 += letter
    if numbers_0 == numbers_1:
        # they are the same
        return -score_decrement
    else:
        return 0

def handle_states(states, score_decrement=5):
    states_1 = states[0]
    states_2 = states[1]
    states_1_is_abbr = len(states_1) == 2
    states_2_is_abbr = len(states_2) == 2
    
    good = True
    if states_1_is_abbr or states_2_is_abbr:
        # if one is abbreviation, make the other an abbreviation, get distance and check threshold
        if states_1_is_abbr and not states_2_is_abbr:
            try:
                states_1_abbr = states_1
                states_2_abbr = abbreviations[states_2]
            except KeyError:
                good = False
        elif states_2_is_abbr and not states_1_is_abbr:
            try:
                states_2_abbr = states_2
                states_1_abbr = abbreviations[states_1]
            except KeyError:
                good = False
        # if both are abbreviations, get distance and check thresh
        if states_1_is_abbr and states_2_is_abbr:
            states_1_abbr = states_1
            states_2_abbr = states_2
            states_distance = edit_distance(states_1_abbr, states_2_abbr, True)
            if states_distance == 0:
                return -5.0
            else:
                return 7.5 * states_distance
        elif good:
            # this means that one one of them was abbreviated
            # abbreviation was successful
            # now, we're comparing the two abbreviations
            states_distance = edit_distance(states_1_abbr, states_2_abbr, True)
            if states_distance == 1:
                return 5.0
            elif states_distance == 2:
                return 15.0
            else:
                return -5
        else:
            return 0
    else:
        # if neither are abbreviations, get distance and check threshold
        states_distance = edit_distance(states_1, states_2, True)
        if states_distance > 2:
            return 15.0
        else:
            return -5.0

def handle_cities(cities, score_decrement=5):
    city_0 = cities[0]
    city_1 = cities[1]
    city_0_is_abbr = len(city_0) < 3
    city_1_is_abbr = len(city_1) < 3

    # if both are abbreviations, then -5 if exactly same
    # if only one is abbreviated, abbreviate the other and get distance
    if city_0_is_abbr or city_1_is_abbr:
        if city_0_is_abbr and not city_1_is_abbr:
            city_0_abbr = city_0
            city_1_abbr = [word[0] for word in city_1.split()]
        elif city_1_is_abbr and not city_0_is_abbr:
            city_1_abbr = city_1
            city_0_abbr = [word[0] for word in city_0.split()]
        # if both are abbreviated, check distance
        if city_0_is_abbr and city_1_is_abbr:
            if edit_distance(city_0, city_1, True) > 0:
                return 10
            else:
                # if distance not positive
                return -5
        else:
            # if only one was abbreviated and the abbreviations are the same
            # then return negative. otherwise, return less positive than
            # if they were both abbreviated to begin with
            if edit_distance(city_0_abbr, city_1_abbr, True) > 0:
                return 3
            else:
                # if distance not positive
                return -4
    # if neither was abbreviated to begin with
    # then get levenshtein distance
    # if greater than thresh, return constant value
    else:
        if edit_distance(city_0, city_1, True) > 2:
            return 10
        else:
            return -11


# for each field, add to score
# if field is empty, it does not count towards score
# IMPORTANT
# make lowercase before passing in
def is_same_person(row1, row2, file_pointer):
    rows = []
    for i in range(len(row1)):
        rows.append([row1[i], row2[i]])

    file_pointer.write(f"Comparing rows...\n{row1}\n{row2}")

    # lower the score, more likely they are the same
    score = 0
    distance = 0

    # ignore 0 and 1
    acct_nums = rows[2]
    if get_distance_between_entries(acct_nums) == 0:
        file_pointer.write(f'Account numbers: -5\n')
        score -= 5
    
    first_names = rows[3]
    if row_is_good(first_names):
        names_score = handle_names(first_names)
        file_pointer.write(f'First names: {names_score}\n')
        score += names_score

    # middle initial
    middle_names = rows[4]
    if row_is_good(middle_names):
        if not is_first_letter_in_row_equal(middle_names):
            score += 20
            file_pointer.write(f'Middle names: 20\n')
    
    last_names = rows[5]
    if row_is_good(last_names):
        names_score = handle_names(last_names)
        file_pointer.write(f'Last names score: {names_score}\n')
        score += names_score

    dates_of_birth = rows[6]
    if row_is_good(dates_of_birth):
        dates_of_birth_distance = get_distance_between_entries(dates_of_birth)
        if dates_of_birth_distance > 2:
            dates_of_birth_score = 10
        else:
            # subtract 5 when distance is 0
            dates_of_birth_score = -(2 - dates_of_birth_distance) * 2.5
        file_pointer.write(f'Dates of birth score: {dates_of_birth_score}\n')
        score += dates_of_birth_score

    # Sex
    # male if starts with M, else Female
    sexes = rows[7]
    if row_is_good(sexes):
        if not is_first_letter_in_row_equal(sexes):
            score += 10
            file_pointer.write('Sexes score: 10\n')
        else:
            score -= 5
            file_pointer.write('Sexes score: -5\n')
    
    current_streets_1 = rows[8]
    if row_is_good(current_streets_1):
        streets_score = handle_streets_1(current_streets_1)
        file_pointer.write(f'Current streets 1 score: {streets_score}\n')
        score += streets_score

    # ignoring all of the letters, if the numbers are the same then good
    current_streets_2 = rows[9]
    if row_is_good(current_streets_2):
        streets_score = handle_streets_2(current_streets_2)
        file_pointer.write(f'Current streets 2 score: {streets_score}\n')
        score += streets_score

    current_cities = rows[10]
    if row_is_good(current_cities):
        # TODO
        # have a function that can handle abbreviations of 2 words
        cities_score = handle_cities(current_cities)
        file_pointer.write(f'Current cities score: {cities_score}\n')
        score += cities_score

    # Current State
    # sometimes the abbreviation is wrong
    # normalize everything to abbreviations
    # take levenshtein distance
    # if distance is 1, then add half rgular amount to score
    states = rows[11]
    if row_is_good(states):
        states_score = handle_states(states)
        file_pointer.write(f'Current states score: {states_score}\n')
        score += states_score
    
    # zip code
    # add distance if it is greater than 1
    current_zip_codes = rows[12]
    if row_is_good(current_zip_codes):
        zip_score = handle_zip_codes(current_zip_codes)
        file_pointer.write(f'Current zip code score: {zip_score}\n')
        score += zip_score

    # FOR NOW
    # ignore 13, 14 and 15
    
    previous_streets_1 = rows[16]
    if row_is_good(previous_streets_1):
        prev_streets_score = handle_streets_1(previous_streets_1)
        file_pointer.write(f'Previous streets 1 score: {prev_streets_score}\n')
        score += prev_streets_score
    
    previous_streets_2 = rows[17]
    if row_is_good(previous_streets_2):
        prev_streets_score = handle_streets_2(previous_streets_2)
        file_pointer.write(f'Previous streets 2 score: {prev_streets_score}\n')
        score += prev_streets_score
    
    previous_cities = rows[18]
    if row_is_good(previous_cities):
        cities_score = handle_cities(current_cities)
        file_pointer.write(f'Previous cities score: {cities_score}\n')
        score += cities_score
    
    previous_states = rows[19]
    if row_is_good(previous_states):
        states_score = handle_states(previous_states)
        file_pointer.write(f'Previous states score: {states_score}\n')
        score += states_score
    
    previous_zip_codes = rows[20]
    if row_is_good(previous_zip_codes):
        zip_score = handle_zip_codes(previous_zip_codes)
        file_pointer.write(f'Previous zip codes score: {zip_score}\n')
        score += zip_score

    file_pointer.write(f'Ending score: {score}\n\n')
    return score

class DummyFilePointer:
    def __init__(self):
        pass
    def write(self, arg):
        pass
    def close(self):
        pass

class PatientRow:
    def __init__(self, row):
        self.row = row
        self.thon_group = None

def test(true_threshold=16.75, display=True, log=True):
    with open('./Patient Matching Data.csv', 'r') as csv_file:
        read = csv.reader(csv_file)
        columns = read.__next__()
        dictionary = {}
        last_row = None
        scores_list = []
        rows = [row for row in read]
    if log:
        log_file = open('./log', 'w+')
    else:
        log_file = DummyFilePointer()

    # guarantees performance
    random.shuffle(rows)

    log_file.write(f"Starting time: {time()}\n")
    for row in rows:
        row = [thing.lower() for thing in row]
        if last_row != None:
            score = is_same_person(row, last_row, log_file)
            is_actually_same = row[0] == last_row[0]
            if display:
                print(f'My score: {score}; Actually same: {is_actually_same}')
            scores_list.append([score, is_actually_same])
        last_row = row
    print()

    log_file.close()

    running_correct = 0.0

    for score in scores_list:
        # if score is below threshold
        if score[0] < true_threshold:
            if score[1]:
                running_correct += 1
        else:
            if not score[1]:
                running_correct += 1
        if display:
            print(score)
    if display:
        print(running_correct / len(scores_list))
    return running_correct / len(scores_list)

# uses sqlite db
def group_patients(true_threshold=16.75, display=False):
    con = sqlite3.connect('patientsDB.db')
    cur = con.cursor()
    cur.execute('SELECT * FROM t')
    rows = [PatientRow(row) for row in cur.fetchall()]
    array_length = len(rows)
    rows = np.asarray(rows)
    con.close()



    # guarantees performance but may take long
    # random.shuffle(rows)

    # with open('./Patient Matching Data.csv', 'r') as csv_file:
    #     read = csv.reader(csv_file)
    #     read.__next__()

    #     rows = []
    #     for row in read:
    # self-named to ensure no naming conflict
    # rows.append(PatientRow(row))

    matrix_size = len(rows)

    # zero initialize an array of size matrix_size
    # scores = np.zeros((matrix_size, matrix_size), dtype=np.single)

    current_index = 0

    log_file = DummyFilePointer()

    # step through rows and compare each row to every other row
    # start at 0
    # compare 0 to 0 -> next
    # compare 0 to 1 -> good
    # etc...
    # at 1
    # compare 1 to 0 -> next
    # compare 1 to 1 -> next
    # compare 1 to 2 -> good

    groups = set()

    correct_total = 0.0
    full_total = 0.0
    for i in range(0, matrix_size):
        for j in range(i + 1, matrix_size):
            if i != j:
                ri = rows[i]
                rj = rows[j]

                actually_same = ri.row[0] == rj.row[0]
                full_total += 1

                score = is_same_person(ri.row, rj.row, log_file)
                # scores[i][j] = score
                # scores[j][i] = score
                if score < true_threshold:
                    # if they match, make new group together
                    # equals operator creates pointers by default

                    if actually_same:
                        correct_total += 1
                    
                    # if this one doesnt have a group
                    if ri.thon_group == None and rj.thon_group == None:
                        # create a new group
                        groups.add(i)
                        ri.thon_group = [i, j]
                        rj.thon_group = ri.thon_group
                    # if you have a group and j doesnt
                    elif ri.thon_group:
                        ri.thon_group.append(j)
                        rj.thon_group = ri.thon_group
                    # if j has a group and you dont
                    elif rj.thon_group:
                        rj.thon_group.append(i)
                        ri.thon_group = rj.thon_group
                    # if both have a group
                    else:
                        # append smaller group to larger group
                        if len(ri.thon_group) < len(rj.thon_group):
                            # remove i group from groups
                            # can this throw an error? <<NOTE>>
                            groups.remove(i)
                            # add ri thon group contents to rj
                            rj.thon_group.extend(ri.thon_group)
                            # del ri.thon_group
                            # set thon group reference
                            ri.thon_group = rj.thon_group
                        else:
                            # remove j group from groups
                            # can this throw an error? <<NOTE>>
                            groups.remove(j)
                            # add rj thon group contents to ri
                            ri.thon_group.extend(rj.thon_group)
                            rj.thon_group = ri.thon_group
                else:
                    if not actually_same:
                        correct_total += 1
        print(f'Finished {i}')
    
    # print out all groups
    group_count = 0
    for group_index in groups:
        group = rows[group_index].thon_group
        print(f'Group #{group_count}')
        for row in group:
            print(rows[row].row)
        print('-----------------------------')
        group_count += 1
    print(f'{group_count} total groups')
    # total = math.factorial(matrix_size)
    print(f'Total Correct: {correct_total}')
    print(f'Total Count: {full_total}')
    print(f'Accuracy: {correct_total / full_total}')

def test_optimal_threshold(starting_threshold, increment=0.015):
    best_thresh = starting_threshold
    best_score = -100
    last_score = -100
    current_thresh = starting_threshold
    current_score = -99
    while current_score >= last_score:
        try:
            current_score = test(current_thresh, False, False)
            if current_score > best_score:
                best_score = current_score
                best_thresh = current_thresh
            current_thresh += increment
            last_score = current_score
            print(f'Current Threshold: {current_thresh}\nCurrent Score: {current_score}\n\n')
            sleep(1)
        except KeyboardInterrupt:
            break
    print(f'Best Threshold: {best_thresh}\nBest Score: {best_score}')