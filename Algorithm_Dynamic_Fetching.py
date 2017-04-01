import operator
import statistics
import random
import os
import math
import time

user_number = 10000
ip_set = {}
page_rank_factor = 0.8
probing_factor = 0.2
rr_record = []
total_sample_size = 10
selection_ratio = 0.8
in_edges = {}
out_edges = {}
out_degree_set = {}
sub_graph = {}
pr_set = {}
avg_out_degree = 0


def calculate_pagerank(pr_value_set, in_edges_set, out_edges_set):
    loop_counter = 0
    break_counter = 0
    while True:
        loop_counter += 1
        for user_id in in_edges_set:
            uid = str(user_id)
            prv = 0.0

            for v in in_edges_set[uid]:
                prv += pr_value_set[str(v)] / out_edges_set[str(v)]
            prv = prv * page_rank_factor + (1 - page_rank_factor) / len(pr_value_set)
            # print('user: ' + uid + '::= ' + str(prv) + ' : ' + str(pr_value_set[uid]))
            if prv != pr_value_set[uid]:
                pr_value_set[uid] = prv
            else:
                break_counter += 1
        if break_counter == len(pr_value_set):
            print('loop times: ', loop_counter)
            break
        break_counter = 0


def init_pagerank(pr_value_set, in_edges_set, out_edges_set):
    for k in in_edges_set:
        pr_value_set[k] = 1 / user_number
    for k in out_edges_set:
        pr_value_set[k] = 1 / user_number
    for key in pr_value_set:
        pr_value_set[key] = 1 / len(pr_value_set)


def page_rank(file_name):
    fr = open(file_name, 'r')
    # fw = open('user1.txt', 'a')
    line = fr.readline()

    for i in range(1, user_number + 1):
        out_degree_set[str(i)] = 0
        out_edges[str(i)] = []
        in_edges[str(i)] = []
    while line != '':
        out_index = line.split('\t')[1].split(' ')[0]
        index = line.split('\t')[0]
        out_degree_set[out_index] += 1
        out_edges[out_index].append(index)
        in_edges[index].append(int(out_index))
        # print(str(out_index) + ': ' + str(out_edges[out_index]))
        # print(in_edges[index])
        line = fr.readline()
    fr.close()
    print('Processing finished\n')
    init_pagerank(pr_set, in_edges, out_degree_set)
    calculate_pagerank(pr_set, in_edges, out_degree_set)
    return sorted(pr_set.items(), key=operator.itemgetter(1), reverse=True)


def init_ip_set():
    for i in range(1, user_number + 1):
        ip_set[str(i)] = [0]


def update_ip(pr_value_set):
    for pr in pr_value_set:
        ip_set[pr].append(pr_value_set[pr])


def update_vertices(file):
    line = file.readline()
    while line != '':
        index = line.split('\t')[0]
        follower = line.split('\t')[1].split(' ')[0]
        if not ip_set[index]:
            ip_set[index] = [0]
        if not ip_set[follower]:
            pr_set[follower] = 1 / float(len(ip_set)+1)
            ip_set[follower] = pr_set[follower]
        if not out_degree_set[follower]:
            out_degree_set[follower] = 1
        line = file.readline()


def network_fetching_algorithm(file_name):
    change_set = {}
    score_set = {}
    vertices_last_round = []
    for v in ip_set:
        vertices_last_round.append(v)
    file_growth = open(file_name, 'r')
    update_vertices(file_growth)
    for v in ip_set:
        if ip_set[v]:
            change_set[v] = statistics.stdev(ip_set[v])
            score_set[v] = (1 - probing_factor) * ip_set[v][-1] + probing_factor * change_set[v]
    probing_set = []
    sorted_score_set = sorted(score_set.items(), key=operator.itemgetter(1), reverse=True)
    # while len(probing_set) <= selection_ratio * total_sample_size:
    counter = 0
    for w in sorted_score_set:
        if counter >= selection_ratio * total_sample_size:
            break
        v = w[0]
        probing_set.append(v)
        vertices_last_round.remove(v)
        counter += 1

    while len(probing_set) < total_sample_size:
        index = random.randint(0, len(vertices_last_round)-1)
        vertex = vertices_last_round[index]
        if vertex not in rr_record:
            probing_set.append(vertex)
            vertices_last_round.remove(vertex)
            rr_record.append(vertex)
    return probing_set


def generate_sub_graph(probing_set):
    for k in probing_set:
        sub_graph[k] = in_edges[k]
        pr_set[k] += page_rank_factor * ip_set[k][-1] / avg_out_degree
        for w in out_edges[k]:
            pr_set[w] -= page_rank_factor * ip_set[k][-1] / avg_out_degree**2
        update_ip(pr_set)


def calculate_real_value(probing_set, date):
    file_full_data = 'full_data_' + str(date) + '_10k.txt'
    file_to_w = 'full_data_new_' + str(date) + '_ss_' + str(total_sample_size) + '_' + str(probing_factor) + '_10k.txt'
    w_path = '/Users/Ethan/PycharmProjects/SocialNetworksAnalysis/full_data_10k'
    file_w_path = os.path.join(w_path, file_to_w)
    f_w = open(file_w_path, 'a')
    pr_value_set = dict(page_rank(file_full_data))
    for v in probing_set:
        f_w.write(v + ': ' + str(pr_value_set[v]) + '\n')
    f_w.close()


def calculate_mse():
    path_sub = '/Users/Ethan/PycharmProjects/SocialNetworksAnalysis/subgraphs_10k'
    path_fd = '/Users/Ethan/PycharmProjects/SocialNetworksAnalysis/full_data_10k'
    sample_size = 80
    for n in range(1, 22):
        file_sub = 'subgraph_' + str(n) + '_ss_' + str(total_sample_size) + '_' + str(probing_factor) + '_10k.txt'
        file_fd = 'full_data_new_' + str(n) + '_ss_' + str(total_sample_size) + '_' + str(probing_factor) + '_10k.txt'
        file_path_sub = os.path.join(path_sub, file_sub)
        file_path_fd = os.path.join(path_fd, file_fd)
        fr_sub = open(file_path_sub, 'r')
        fr_fd = open(file_path_fd, 'r')
        line_sub = fr_sub.readline()
        line_fd = fr_fd.readline()
        sum_diff = 0.0
        counter = 0
        while line_sub != '' and line_fd != '':
            v_sub = float(line_sub.split(' ')[1])
            v_fd = float(line_fd.split(' ')[1])
            sum_diff += (v_sub - v_fd)**2
            line_sub = fr_sub.readline()
            line_fd = fr_fd.readline()
            counter += 1
            if counter == 80:
                break
        fr_sub.close()
        fr_fd.close()
        mse = math.sqrt(sum_diff / float(sample_size))
        # 'Day ' + str(n) + ': ' +
        print(str(mse))


def generate_full_data():
    for i in range(1, 22):
        file_to_read = 'timestamp_' + str(i) + '_1000.txt'
        file_to_read_old = 'full_data_' + str(i-1) + '_1000.txt'
        file_to_write = 'full_data_' + str(i) + '_1000.txt'
        fw = open(file_to_write, 'a')
        fr_2 = open(file_to_read, 'r')
        fr_1 = open(file_to_read_old, 'r')
        fw.write(fr_1.read())
        fw.write(fr_2.read())
        fw.close()
        fr_1.close()
        fr_2.close()


start_time = time.time()
original_set = page_rank('timestamp_0_10k.txt')
pr_set = dict(original_set)
init_ip_set()
update_ip(pr_set)
avg_out_degree = sum(out_degree_set.values()) / float(len(out_degree_set))
for n in range(1, 22):
    # user_set_for_random = []
    # file_to_write = 'full_data_' + str(n) + '_' + str(probing_factor) + '_10k.txt'
    # path = '/Users/Ethan/PycharmProjects/SocialNetworksAnalysis/full_data'
    # file_path = os.path.join(path, file_to_write)
    # fr = open(file_path)
    # new_line = fr.readline()
    # while new_line != '':
    #     u_name = new_line.split(':')[0]
    #     user_set_for_random.append(u_name)
    #     new_line = fr.readline()
    # fr.close()

    file_to_read = 'timestamp_' + str(n) + '_10k.txt'
    result = network_fetching_algorithm(file_to_read)
    generate_sub_graph(result)  # should be result
    file_to_write = 'subgraph_' + str(n) + '_ss_' + str(total_sample_size) + '_' + str(probing_factor) + '_10k.txt'
    path = '/Users/Ethan/PycharmProjects/SocialNetworksAnalysis/subgraphs_10k'
    file_path = os.path.join(path, file_to_write)
    fw = open(file_path, 'a')
    for i in result:   # should be result
        fw.write(i + ': ' + str(pr_set[i]) + '\n')
    fw.close()
    calculate_real_value(result, n)

calculate_mse()
print(time.time() - start_time)
# while new_line != '':
#     if new_line.split('\t')[0] in result:


# for pr in original_set:
#     print(pr[0])
# print('------------------------\n')
# for ve in result:
#     print(ve)

