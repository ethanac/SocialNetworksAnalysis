import random

fileName_07 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2007/release-flickr-links.txt'
fileName_08 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2008/flickr-growth-sorted.txt'
file_raw_07 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2007/raw_07.txt'
file_raw_07_1000 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2007/raw_07_1000.txt'
file_raw_08 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2008/raw_08.txt'
file_trimmed_07 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2007/trimmed_07.txt'
file_trimmed_07_1000 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2007/trimmed_07_1000.txt'
file_trimmed_08 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2008/trimmed_08.txt'
file_randtimed_07 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2007/timestamped_07.txt'
file_randtimed_07_1000 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2007/timestamped_07_1000.txt'
file_randgrowtimed_07 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2007/all_timestamped_07.txt'
file_randgrowtimed_07_1000 = '/Users/Ethan/Downloads/SocialNetworks_Datasets/Flickr/2007/all_timestamped_07_1000.txt'
pr_factor = 0.8
user_number = 1000


# Step 1
def get_users(file_read, file_write):
    f = open(file_read, 'r')
    fw = open(file_write, 'a')
    line = f.readline()
    counter = 0
    while int(line.split('\t')[0]) <= user_number:
        # line = line.replace('\n', '') + ' 20051102\n'
        fw.write(line)
        line = f.readline()
        counter += 1
    print('Total line: ', counter)
    # 2886884
    fw.close()
    f.close()


# Step 2
def trim_record(file_read, file_write):
    f = open(file_read, 'r')
    fw = open(file_write, 'a')
    line = f.readline()
    counter = 0
    while line != '':
        follower = line.split('\t')[1].split(' ')[0]
        if int(follower) <= user_number:
            fw.write(line)
            counter += 1
        line = f.readline()
    # 1046884
    print('After trimmed: ', counter)
    fw.close()
    f.close()


# Step 3
def sort_follower(file_read):
    f = open(file_read, 'r')
    follower_set = []
    line = f.readline()
    counter = 0
    user_id = 1
    while line != '':
        if int(line.split('\t')[0]) != user_id:
            user_id = int(line.split('\t')[0])
            follower_set.append(counter)
            counter = 0
        counter += 1
        line = f.readline()
    follower_set.sort(key=int, reverse=True)
    for i in range(100):
        print(follower_set[i])
    f.close()


# getUsers(fileName_08, file_raw_08)
# trimRecord(file_raw_08, file_trimmed_08)
# sortFollower(file_trimmed_07)
def init_timestamps():
    f = open(file_trimmed_07_1000, 'r')
    fw = open(file_randtimed_07_1000, 'a')
    line = f.readline()
    while line != '':
        line = line.replace('\n', '') + ';' + str(random.randint(0, 2)) + '\n'
        fw.write(line)
        line = f.readline()
    fw.close()
    f.close()


def add_full_timestamps():
    f = open(file_randtimed_07_1000, 'r')
    fw = open(file_randgrowtimed_07_1000, 'a')
    acounter = 0
    new_counter = 0
    line = f.readline()
    while line != '':
        acounter += 1
        if int(line.split(';')[1]) != 0:
            line = line.split(';')[0] + ';' + str(random.randint(1, 21)) + '\n'
            new_counter += 1
        fw.write(line)
        line = f.readline()
    fw.close()
    f.close()
    f = open(file_randgrowtimed_07)
    for i in range(1000):
        print(f.readline())
    f.close()


def create_files_by_timestamp():
    f = open(file_randgrowtimed_07_1000, 'r')
    line = f.readline()
    while line != '':
        f_name = 'timestamp_' + line.replace('\n', '').split(';')[1] + '_1000.txt'
        with open(f_name, 'a') as fw:
            fw.write(line)
        line = f.readline()
    f.close()
    print('finished')


# def cc_pagerank(set_of_out_degree):
#     pr_old = 0.0
#     pr_new = 0.0
#     f = open('timestamp_0.txt', 'r')
#     line = f.readline()
#     od = set_of_out_degree[0]
#     for v in range(1, user_number + 1):
#         if line.split('\t')[0] == str(v):
#             calculate_pagerank()


def process_data():
    get_users(fileName_07, file_raw_07_1000)
    trim_record(file_raw_07_1000, file_trimmed_07_1000)
    sort_follower(file_trimmed_07_1000)
    init_timestamps()
    add_full_timestamps()
    create_files_by_timestamp()


# u_id = 0
# while new_line != '':
#     fw.write(new_line.split('\t')[1].split(' ')[0] + '\n')
#     # '',' + new_line.split('\t')[1].split(' ')[0] + '\n')
#     new_line = fr.readline()
# fw.close()
# fr.close()

# size_of_out_edge = []
# for k, v in out_edge.items():
#     size_of_out_edge.append(v)
# size_of_out_edge.sort()
# sum_out_edge = 0
# for s in size_of_out_edge:
#     print(str(s))
#     sum_out_edge += s
# print('Average size of out edges is ', sum_out_edge / 10000.0)
