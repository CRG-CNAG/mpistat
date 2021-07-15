'''
functions for getting mpistat data
in a clickhouse database
'''

import pwd
import grp
import bidict
import clickhouse_driver
from memorised.decorators import memorise

import treemap_config

# global bidicts for users and groups
GROUPS = bidict.bidict()

# the primary key is username
USERS = bidict.bidict()


def get_gid(group):
    '''
    get the gid for a particular group
    '''
    try:
        gid = GROUPS.inverse[group]
        return gid
    except KeyError:
        try:
            gid = grp.getgrnam(group)[2]
            GROUPS[gid] = group
            return gid
        except KeyError:
            GROUPS[group] = group
            return group


def get_group(gid):
    '''
    get the group for a particular gid
    '''
    try:
        # this line triggers a false positive in pylint
        # see https://github.com/PyCQA/pylint/issues/1498
        #pylint: disable=unsubscriptable-object
        group = GROUPS[gid]
        return group
    except KeyError:
        try:
            group = grp.getgrgid(gid)[0]
            GROUPS[gid] = str(group)
            return str(group)
        except KeyError:
            GROUPS[gid] = str(gid)
            return str(gid)


def get_uid(username):
    '''
    get the uid for a particular username
    '''
    try:
        uid = USERS.inverse[username]
        return uid
    except KeyError:
        try:
            uid = pwd.getpwnam(username)[2]
            USERS[uid] = username
            return uid
        except KeyError:
            USERS[username] = username
            return username


def get_username(uid):
    '''
    get the username for a particular uid
    '''
    try:
        # this line triggers a false positive in pylint
        # see https://github.com/PyCQA/pylint/issues/1498
        #pylint: disable=unsubscriptable-object
        username = USERS[uid]
        return username
    except KeyError:
        try:
            username = pwd.getpwuid(uid)[0]
            USERS[uid] = username
            return username
        except KeyError:
            USERS[uid] = str(uid)
            return str(uid)


def get_bytes_str(nbytes):
    '''
    pretty print a bytes value like the -h parameter to du does
    '''
    if nbytes < 1024:
        return str(nbytes)
    if nbytes < 1024*1024:
        return "{:.3f}".format(1.0*nbytes/1024.0)+'k'
    if nbytes < 1024*1024*1024:
        return "{:.3f}".format(1.0*nbytes/(1024*1024.0))+'M'
    if nbytes < 1024*1024*1024*1024:
        return "{:.3f}".format(1.0*nbytes/(1024*1024.0*1024))+'G'
    return "{:.3f}".format(1.0*nbytes/(1024*1024.0*1024*1024))+'T'


def get_num_str(num):
    '''
    print a number short form based on powers of 1000
    '''
    if num < 1000:
        return '{:.3f}'.format(num)
    if num < 1000*1000:
        return '{:.3f}'.format(1.0*num/1000.0)+'k'
    if num < 1000*1000*1000:
        return '{:.3f}'.format(1.0*num/(1000*1000.0))+'M'
    return '{:.3f}'.format(1.0*num/(1024*1024.0*1024))+'B'


def path_depth(path):
    '''
    get the depth of the given path
    '''
    return len(path.split('/')) - 1


def get_databases():
    '''
    returns the set available databases
    '''
    click = clickhouse_driver.Client(
        treemap_config.CLICK_HOST,
        compression=True)
    rows = click.execute('show databases')
    databases = set()
    for row in rows:
        databases.add(row[0])
    databases.remove('default')
    databases.remove('system')
    databases.remove('_temporary_and_external_tables')
    return databases


def get_database(tag):
    '''
    get the latest database with the given tag
    uses the default tag if None is passed
    '''
    if tag is None:
        tag=treemap_config.DEFAULT_TAG
    qry = '''
        select name
        from system.databases
        where name like '{}_%'
        order by name desc
        limit 1
    '''
    qry = qry.format(tag)
    click = clickhouse_driver.Client(
        treemap_config.CLICK_HOST,
        compression=True)
    return click.execute(qry)[0][0]


def get_click(database):
    '''
    get a clickhouse client object for the
    required database
    '''
    return clickhouse_driver.Client(
        treemap_config.CLICK_HOST,
        database=database,
        compression=True)


def filter_qry(
        group, user,
        modified_before, modified_after,
        accessed_before, accessed_after,
        size_less_than, size_greater_than,
        suffix, regex):
    '''
    create the text to append to queries
    based on the filters in args
    **** THIS IS A SECURITY RISK CURRENTLY ****
    need to figure out best way to do query parameters
    for clickhouse
    '''
    qry = ''
    if group is not None:
        qry += ' and gid={}'.format(get_gid(group))
    if user is not None:
        qry += ' and uid={}'.format(get_uid(user))
    if modified_before is not None:
        qry += ' and mtime < {}'.format(modified_before)
    if modified_after is not None:
        qry += ' and mtime > {}'.format(modified_after)
    if accessed_before is not None:
        qry += ' and atime < {}'.format(accessed_before)
    if accessed_after is not None:
        qry += ' and atime > {}'.format(accessed_after)
    if size_less_than is not None:
        qry += ' and size < {}'.format(size_less_than)
    if size_greater_than is not None:
        qry += ' and size > {}'.format(size_greater_than)
    if suffix is not None:
        qry += " and suffix='{}'".format(suffix)
    if regex is not None:
        qry += " and match(full_path,'{}')".format(regex)
    return qry


@memorise(mc_servers=treemap_config.MC_SERVERS)
def subdirs(database, path):
    '''
    get a list of directories immediately underneath the given path
    also need to pass in a clickhouse connection object
    '''

    # get the depth of the given path
    depth = path_depth(path)
    qry = '''
        select full_path as path
        from directories
        where full_path like '{}/%'
        and depth={}'''
    qry = qry.format(path, depth+1)
    rows = get_click(database).execute(qry)
    result = list()
    for row in rows:
        result.append(row[0])
    return result


@memorise(mc_servers=treemap_config.MC_SERVERS)
def subtree_sums(
        database, path, group, user,
        modified_before, modified_after, accessed_before, accessed_after,
        size_less_than, size_greater_than, suffix, regex):
    '''
    generate clickhouse query to get subtree sums
    for the given path and filters passed on via the
    args dictionary
    '''
    subqry = '''
        (select blocks, atime_cost,
            count(inode) as links
        from files
        where full_path like '{}/%'
    '''.format(path)
    qry = '''
        select
            sum(blocks*512) as tot_size,
            sum(links) as tot_num,
            sum(atime_cost) as tot_atime_cost
        from {}
    '''
    subqry += filter_qry(
        group, user,
        modified_before, modified_after, accessed_before, accessed_after,
        size_less_than, size_greater_than, suffix, regex)
    subqry += 'group by blocks, atime_cost)'
    qry = qry.format(subqry)
    return get_click(database).execute(qry)[0]


@memorise(mc_servers=treemap_config.MC_SERVERS)
def star_dot_star(
        database, path, group, user,
        modified_before, modified_after, accessed_before, accessed_after,
        size_less_than, size_greater_than, suffix, regex):
    '''
    get sums for the given path and filters passed on via the args dictionary
    '''
    subqry = '''
        (select blocks, atime_cost,
            count(inode) as links
        from files
        where directory='{}'
    '''.format(path)
    qry = '''
        select
            sum(blocks*512) as tot_size,
            sum(links) as tot_num,
            sum(atime_cost) as tot_atime_cost
        from {}
    '''
    subqry += filter_qry(
        group, user,
        modified_before, modified_after,
        accessed_before, accessed_after,
        size_less_than, size_greater_than,
        suffix, regex)
    subqry += 'group by blocks, atime_cost)'
    qry = qry.format(subqry)
    return get_click(database).execute(qry)[0]


@memorise(mc_servers=treemap_config.MC_SERVERS)
def get_subdir_data(
        database, path, group, user,
        modified_before, modified_after, accessed_before, accessed_after,
        size_less_than, size_greater_than, suffix, regex):
    '''
    get data for subdirs
    '''

    # initialise the data
    data = {
        'database': database,
        'path': path,
        'size': 0,
        'num_files': 0,
        'atime_cost': 0,
        'children': []}
    children = data['children']
    tot_size = 0
    tot_num_files = 0
    tot_atime_cost = 0

    # get the *.* data if any
    size, num_files, atime_cost = star_dot_star(
        database, path, group, user,
        modified_before, modified_after, accessed_before, accessed_after,
        size_less_than, size_greater_than, suffix, regex)
    if num_files > 0:
        tot_size += size
        tot_num_files += num_files
        tot_atime_cost += atime_cost
        children.append({
            'name': '*.*',
            'size': size,
            'num_files': num_files,
            'atime_cost': atime_cost})

    # get the totals for each subdir
    for directory in subdirs(database, path):
        size, num_files, atime_cost = subtree_sums(
            database, directory, group, user,
            modified_before, modified_after, accessed_before, accessed_after,
            size_less_than, size_greater_than, suffix, regex)
        if num_files > 0:
            tot_size += size
            tot_num_files += num_files
            tot_atime_cost += atime_cost
            children.append({
                'name': directory.rsplit('/', 1)[-1],
                'size': size,
                'num_files': num_files,
                'atime_cost': atime_cost})
    data['size'] = tot_size
    data['num_files'] = tot_num_files
    data['atime_cost'] = tot_atime_cost
    return data


@memorise(mc_servers=treemap_config.MC_SERVERS)
def by_user(
        database, path, group,
        user, modified_before, modified_after, accessed_before, accessed_after,
        size_less_than, size_greater_than, suffix, regex, order_by, limit):
    '''
    get usage by user
    '''

    # get the total sums for this directory
    size, num_files, atime_cost = subtree_sums(
        database, path, group, user,
        modified_before, modified_after, accessed_before, accessed_after,
        size_less_than, size_greater_than, suffix, regex)
    data = {
        'database': database,
        'path': path,
        'size': size,
        'num_files': num_files,
        'atime_cost': atime_cost,
        'children': []}
    children = data['children']

    # prepare the query to get the by_user data
    qry = '''
        select
            uid,
            sum(blocks*512) as size,
            count(*) as num_files,
            sum(atime_cost) as atime_cost
        from files
        where full_path like '{}/%'
    '''
    qry = qry.format(path)
    qry += filter_qry(
        group, user,
        modified_before, modified_after,
        accessed_before, accessed_after,
        size_less_than, size_greater_than,
        suffix, regex)
    qry += '''
        group by uid
        order by {} desc
        limit {}
    '''

    # execute it and process the results
    qry = qry.format(order_by, limit)
    rows = get_click(database).execute(qry)
    for row in rows:
        children.append({
            'name': get_username(row[0]),
            'size': row[1],
            'num_files': row[2],
            'atime_cost': row[3]})
    return data


@memorise(mc_servers=treemap_config.MC_SERVERS)
def by_group(
        database, path,
        group, user,
        modified_before, modified_after,
        accessed_before, accessed_after,
        size_less_than, size_greater_than,
        suffix, regex,
        order_by, limit):
    '''
    get usage by group
    '''

    # get the total sums for this directory
    size, num_files, atime_cost = subtree_sums(
        database, path,
        group, user,
        modified_before, modified_after,
        accessed_before, accessed_after,
        size_less_than, size_greater_than,
        suffix, regex)
    data = {
        'database': database,
        'path': path,
        'size': size,
        'num_files': num_files,
        'atime_cost': atime_cost,
        'children': []}
    children = data['children']

    # prepare the query to get the by_group data
    qry = '''
        select
            gid,
            sum(blocks*512) as size,
            count(*) as num_files,
            sum(atime_cost) as atime_cost
        from files
        where full_path like '{}/%'
    '''
    qry = qry.format(path)
    qry += filter_qry(
        group, user,
        modified_before, modified_after, accessed_before, accessed_after,
        size_less_than, size_greater_than, suffix, regex)
    qry += '''
        group by gid
        order by {} desc
        limit {}
    '''
    qry = qry.format(order_by, limit)

    # execute it and process the results
    rows = get_click(database).execute(qry)
    for row in rows:
        children.append({
            'name': str(get_group(row[0])),
            'size': row[1],
            'num_files': row[2],
            'atime_cost': row[3]})
    return data


@memorise(mc_servers=treemap_config.MC_SERVERS)
def by_suffix(
        database, path, group, user,
        modified_before, modified_after, accessed_before, accessed_after,
        size_less_than, size_greater_than, suffix, regex, order_by, limit):
    '''
    get usage by suffix
    '''
    # get the total sums for this directory
    size, num_files, atime_cost = subtree_sums(
        database, path,
        group, user,
        modified_before, modified_after,
        accessed_before, accessed_after,
        size_less_than, size_greater_than,
        suffix, regex)
    data = {
        'database': database,
        'path': path,
        'size': size,
        'num_files': num_files,
        'atime_cost': atime_cost,
        'children': []}
    children = data['children']

    # prepare the query to get the by_suffix data
    qry = '''
       select
           suffix,
           sum(blocks*512) as size,
           count(*) as num_files,
           sum(atime_cost) as atime_cost
       from files
       where full_path like '{}/%'
    '''
    qry = qry.format(path)
    qry += filter_qry(
        group, user,
        modified_before, modified_after, accessed_before, accessed_after,
        size_less_than, size_greater_than, suffix, regex)
    qry += '''
       group by suffix
       order by {} desc
       limit {}
    '''
    qry = qry.format(order_by, limit)

    # excecute it and process the results
    rows = get_click(database).execute(qry)
    for row in rows:
        children.append({
            'name': row[0],
            'size': row[1],
            'num_files': row[2],
            'atime_cost': row[3]})
    return data
