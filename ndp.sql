set global max_connections = 2000;

drop table if exists worker;
drop table if exists task;
drop table if exists task_log;
drop procedure if exists reserve_tasks;

create table worker (
    worker_id varchar(255) not null,
    task_id bigint not null,
    host varchar(255),
    batch_id bigint not null,
    unique key (host),
    unique key (task_id)
);

-- partition on task table?
create table task (
    task_id bigint primary key not null AUTO_INCREMENT,
    host text,
    commands text,
    status enum('SUBMITTED', 'RESERVED', 'STARTED', 'DONE') default 'SUBMITTED',
    started timestamp null,
    ended timestamp null,
    scheduled timestamp null
);

create table task_log (
    worker_id varchar(255) not null,
    task_id bigint not null,
    status enum('SUBMITTED', 'RESERVED', 'STARTED', 'DONE'),
    primary key (worker_id, task_id, status)
);

delimiter $$ ;

create procedure reserve_tasks(
    in _worker_id varchar(255),
    in _done_task_ids text,
    in _max_worker int)
begin
    declare _batch_id bigint;
    declare _n int;

    set _batch_id = uuid_short();

    start transaction;
        delete from worker where find_in_set(task_id, _done_task_ids);

        select _max_worker - count(*) into _n from worker where worker_id = _worker_id;

        insert ignore into worker
            (worker_id, task_id, host, batch_id)
        select
            _worker_id, task_id, host, _batch_id
        from
            task
        where
            status = 'SUBMITTED'
        order by
            1 asc
        limit
            _n;
    commit;

    start transaction;
        update
            task
        set
            status = 'STARTED'
        where
            task_id in (
                select task_id
                from worker
                where worker_id = _worker_id and batch_id = _batch_id
            );

        insert into task_log
            (worker_id, task_id, status)
        select
            worker_id, task_id, 'STARTED'
        from
            worker
        where
            worker_id = _worker_id
            and batch_id = _batch_id;

        select
            task_id, host
        from
            task
        where
            task_id in (
                select task_id
                from worker
                where worker_id = _worker_id and batch_id = _batch_id
            );
    commit;

    do sleep(0.2);
end $$

delimiter ; $$
