#coding=utf-8

import os

def get_part_node(dbname, parition_number, scale, node_ip, master_ip, pgbench_db):
    # 默认在master上执行
    command ='ssh ' + master_ip + ' "/usr/local/pgsql/bin/psql -c \'select part_name, node_id from  shardman.partitions   ;\' -d ' + dbname + '"'
    print command
    msg = os.popen(command).read()
    part_node_a = {}
    for i in  msg.split('\n')[2:partition_number+2]:
        part_node_a.setdefault(i.split('|')[1].strip(),[]).append(i.split('|')[0].split('_')[2].strip())
    part_node_b = {}
    for i in  msg.split('\n')[partition_number+2:2*partition_number+2]:
        part_node_b.setdefault(i.split('|')[1].strip(),[]).append(i.split('|')[0].split('_')[2].strip())
    part_node_t = {}
    for i in  msg.split('\n')[2*partition_number+2:3*partition_number+2]:
        part_node_t.setdefault(i.split('|')[1].strip(),[]).append(i.split('|')[0].split('_')[2].strip())
    last_node = ''
    command = "/usr/local/pgsql/bin/psql -c 'select shardman.get_my_id();' -d " + dbname
    print command
    msg = os.popen(command).read()
    myid = msg.split('\n')[2].strip()
    print myid     
    k = 1
    for key, value in part_node_a.items():
        if key == myid:
            b = part_node_b.get(key)
            t = part_node_t.get(key)
            i = 0 
            for nid in value:
                if k == 1:
                    k += 1
                    continue
                command = '/usr/local/pgsql/bin/psql -c "truncate table pgbench_accounts_' + nid + ';" -d ' + dbname
                print command
                os.system(command)
                command = '/usr/local/pgsql/bin/psql -c "truncate table pgbench_branches_' + b[i] + ';" -d ' + dbname
                print command
                os.system(command)
                command = '/usr/local/pgsql/bin/psql -c "truncate table pgbench_tellers_' + t[i] + ';" -d ' + dbname
                print command
                os.system(command)
                command = '/usr/local/pgsql/bin/pgbench -i -s ' + str(scale) + ' -d ' + dbname + ' --partition-number ' + str(partition_number) + ' --partition-index ' + str(nid) + '_' + str(b[i]) + '_' + str(t[i])
                i += 1
                print command 
                os.system(command)
                '''
                command = '/usr/local/pgsql/bin/pg_dump -a -t pgbench_accounts ' + pgbench_db +' | /usr/local/pgsql/bin/psql -d ' + dbname
                print command
                os.system(command)
                '''  
        '''
        n_ip = ''
        for (node, pid) in part_node.items():
            if str(i) in pid:
                n_ip = node_ip[node]
                last_node = n_ip       
        command = 'scp /tmp/pgbench_accounts_' + str(i)  + ' ' + n_ip + ':/tmp/'
        os.system(command)      
        print command
        command = 'ssh '+ n_ip + ' "cat /tmp/pgbench_accounts_' + str(i) + " | /usr/local/pgsql/bin/psql  -c 'copy pgbench_accounts from stdin ' -d " + dbname + '"'
        print command
        os.system(command)

    # copy pgbench_branches & pgbench_tellers    
    command = '/usr/local/pgsql/bin/psql -c "copy pgbench_branches to \'/tmp/pgbench_branches\';" -d ' + dbname
    print command
    os.system(command)
    command = '/usr/local/pgsql/bin/psql -c "copy pgbench_tellers to \'/tmp/pgbench_tellers\';" -d ' + dbname
    print command
    os.system(command)
    command = 'scp /tmp/pgbench_branches '  + last_node + ':/tmp/'
    print command
    os.system(command)
    command = 'scp /tmp/pgbench_tellers '  + last_node + ':/tmp/'
    print command
    os.system(command) 
    # copy pgbench_insert.py to last_node
    command = 'scp /tmp/pgbench_insert.py '  + last_node + ':/tmp/'
    print command
    os.system(command)

    # insert into pgbench_branches & tellers
    command = 'ssh ' + last_node + ' "python /tmp/pgbench_insert.py ' + last_node + ' postgres 5432 liuzhi 123linux "'  
    print command
    os.system(command)

    print '*'*20
    '''
def get_node_ip(dbname, node_number, master_ip):
    msg = os.popen('ssh ' + master_ip  + ' "/usr/local/pgsql/bin/psql -c \'select * from  shardman.nodes;\' -d ' + dbname + '"').read()
    
    node_ip = {}
    for line in msg.split('\n')[2:node_number+2]:
        node_ip[line.split('|')[0].strip()] = line.split('|')[2].split(' ')[1][5:].strip()
    print node_ip
    return node_ip


def reshard_db(dbname, number, master_ip):
    command = '/usr/local/pgsql/bin/psql -c "select shardman.rm_table(\'pgbench_accounts\'); " -d ' + dbname
    print command
    os.system(command)
    command = '/usr/local/pgsql/bin/psql -c "select shardman.rm_table(\'pgbench_branches\'); " -d ' + dbname
    os.system(command)
    command = '/usr/local/pgsql/bin/psql -c " select shardman.rm_table(\'pgbench_tellers\'); " -d ' + dbname
    os.system(command)
    command = '/usr/local/pgsql/bin/psql -c "select shardman.create_hash_partitions(\'pgbench_accounts\', \'aid\',' + str(number) + ' , 0); " -d ' + dbname
    os.system(command)
    command = '/usr/local/pgsql/bin/psql -c "select shardman.create_hash_partitions(\'pgbench_branches\', \'bid\',' + str(number) + ' , 0); " -d ' + dbname
    os.system(command)
    command = '/usr/local/pgsql/bin/psql -c "select shardman.create_hash_partitions(\'pgbench_tellers\', \'tid\',' + str(number) + ' , 0); " -d ' + dbname
    os.system(command)
    print command
   

if __name__ == '__main__':
   
    dbname = 'postgres' 
    pgbench_db = 'pgbench_db'
    partition_number = 12
    node_number = 6
    scale = 7000
    master_ip = '192.168.1.1'
    #reshard_db(dbname, partition_number, master_ip)
    node_ip = get_node_ip(dbname, node_number, master_ip)
    get_part_node(dbname, partition_number, scale, node_ip, master_ip, pgbench_db)
