import json
import random
import multiprocessing
import math
import datetime
import time

def get_random_int():
    return str(random.randint(0, 100))

def get_random_float():
    return round(random.uniform(0, 1), 2)

def get_random_city():
    cities = ['vaslui', 'valea lupului']
    return random.choice(cities)

def get_random_direction():
    directions = ['N', 'S', 'E', 'W']
    return random.choice(directions)

def get_random_date():
    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date(2023, 4, 2)

    return str(start_date + datetime.timedelta(days=random.randint(0, (end_date - start_date).days)))

def get_number_allowed_operator():
    return ['<', '>', '=', '<=', '>=', '!=', '~']

def get_string_allowed_operator():
    return ['=', '!=']

FIELDS = {
    'stationid': get_random_int,
    'city': get_random_city,
    'temp': get_random_int,
    'wind': get_random_int
}

ALLOWED_OPERATORS = {
    'stationid': get_number_allowed_operator,
    'city': get_string_allowed_operator,
    'temp': get_number_allowed_operator,
    'wind': get_number_allowed_operator
}

def generate_publication():
    publication = {}
    for field in FIELDS:
        publication[field] = FIELDS[field]()
    return publication

def generate_subscription(constraints, allowed_fields):
    fields = random.sample(allowed_fields, random.randint(1, len(allowed_fields)))
    subscription = {}
    for field in fields:
        subscription[field] = {
            'operator': None,
            'value': FIELDS[field]()
        }
    for constraint in constraints:
        subscription[constraint['field']] = {
            'operator': constraint['operator'],
            'value': FIELDS[constraint['field']]()
        }
    return subscription

def update_subscription(subscription, constraints, allowed_operators):
    for constraint in constraints:
        subscription[constraint['field']]['operator'] = constraint['operator']
    for field in subscription:
        if subscription[field]['operator'] is None:
            subscription[field]['operator'] = random.choice(allowed_operators[field])
    return subscription

def work1(Q_in, Q_out, results, allowed):
    while not Q_in.empty():
        item = Q_in.get()
        if item['type'] == 'pub':
            Q_out.put(((item['index'], generate_publication())))
        else:
            Q_out.put((item['index'], generate_subscription(item['constraints'], allowed)))
        Q_in.task_done()

def work2(Q_in, Q_out, results, allowed):
    while not Q_in.empty():
        item = Q_in.get()
        if item['type'] == 'sub':
            Q_out.put((item['index'], update_subscription(results[item['index']], item['constraints'], allowed)))
        Q_in.task_done()

def generate(Q_in, Q_out, work, results, allowed):
    threads = []
    for i in range(config['threads']):
        x = multiprocessing.Process(target=work, args=(Q_in, Q_out, results, allowed))
        threads.append(x)
        x.start()

    Q_in.join()

    while not Q_out.empty():
        item = Q_out.get()
        results[item[0]] = item[1]

if __name__ == '__main__':

    start_time = time.time()

    with open('config.json') as config_file:
        config = json.load(config_file)

    to_generate = [{} for _ in range(config['publications'] + config['subscriptions'])]
    results = [{} for _ in range(config['publications'] + config['subscriptions'])]
    pub_indices = [i for i in range(config['publications'])]
    sub_indices = [i for i in range(config['publications'], config['publications'] + config['subscriptions'])]
    for index in pub_indices:
        to_generate[index]['index'] = index
        to_generate[index]['type'] = 'pub'
    for index in sub_indices:
        to_generate[index]['index'] = index
        to_generate[index]['type'] = 'sub'
        to_generate[index]['constraints'] = []

    allowed_fields = list(FIELDS.keys())

    for constraint in config['constraints']:
        if constraint['type'] == 'operator':
            continue
        percent = int(constraint['percent'][1:])
        if constraint['percent'][0] == '<':
            percent = random.randint(0, percent - 1)
        elif constraint['percent'][0] == '>':
            percent = random.randint(percent + 1, 100)
        indices = random.sample(sub_indices, math.ceil(percent * len(sub_indices) / 100))
        q_constaint = {
                'field': constraint['field'],
                'operator': None
            }
        
        if constraint['field'] in allowed_fields:
            allowed_fields.remove(constraint['field'])

        for index in indices:
            to_generate[index]['constraints'].append(q_constaint)

    Q_in = multiprocessing.JoinableQueue()
    Q_out = multiprocessing.Manager().Queue()
    for i in to_generate:
        Q_in.put(i)

    generate(Q_in, Q_out, work1, results, allowed_fields)

    field_indices = {}
    allowed_operators = {}
    for field in FIELDS.keys():
        field_indices[field] = []
        allowed_operators[field] = ALLOWED_OPERATORS[field]()

    for index in sub_indices:
        for field in results[index]:
            field_indices[field].append(index)
        to_generate[index]['constraints'] = []

    for constraint in config['constraints']:
        if constraint['type'] == 'frequency':
            continue
        percent = int(constraint['percent'][1:])
        if constraint['percent'][0] == '<':
            percent = random.randint(0, percent - 1)
        elif constraint['percent'][0] == '>':
            percent = random.randint(percent + 1, 100)
        indices = random.sample(field_indices[constraint['field']], math.ceil(percent * len(field_indices[constraint['field']]) / 100))
        q_constaint = {
                'field': constraint['field'],
                'operator': constraint['operator']
            }
        
        if constraint['operator'] in allowed_operators[constraint['field']]:
            allowed_operators[constraint['field']].remove(constraint['operator'])

        for index in indices:
            to_generate[index]['constraints'].append(q_constaint)

    Q_in = multiprocessing.JoinableQueue()
    Q_out = multiprocessing.Manager().Queue()
    for index in sub_indices:
        Q_in.put(to_generate[index])

    generate(Q_in, Q_out, work2, results, allowed_operators)

    final_results = {
        'publications': results[:config['publications']],
        'subscriptions': results[config['publications']:]
    }

    end_time = time.time()

    print('Time taken: ', end_time - start_time)

    with open('results.json', 'w') as results_file:
        json.dump(final_results, results_file, indent=4)