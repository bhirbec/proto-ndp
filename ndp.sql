set global max_connections = 2000;

drop table if exists worker;
drop table if exists task;
drop table if exists task_log;
drop procedure if exists reserve_task;
drop function if exists lock_task;

create table worker (
    worker_id varchar(255) not null,
    task_id bigint not null,
    host varchar(255),
    unique key (worker_id),
    unique key (host),
    unique key (task_id)
);

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

create procedure reserve_task(in _worker_id varchar(255))
begin
    declare _task_id bigint;
    declare _host text;
    declare _done int default false;
    declare _ok int default false;

    declare exit handler for sqlexception begin
        rollback;
    end;

    declare exit handler for sqlwarning begin
        rollback;
    end;

    start transaction;

    -- declare _cur cursor for
    select
        task_id, host
    into
        _task_id, _host
    from
        task
    where
        status = 'SUBMITTED'
    order by
        1 asc
    limit
        1
    for update;

    if _task_id then
        update task set status = 'STARTED' where task_id = _task_id;

        insert into worker (worker_id, task_id, host)
        values (_worker_id, _task_id, _host);

        insert into task_log (task_id, worker_id)
        values (_task_id, _worker_id);
    end if;

    commit;

    do sleep(0.2);

    if _task_id then
        select _task_id as task_id, _host as host;
    end if;

    -- declare continue handler for not found set _done = true;
    -- open _cur;

    -- read_loop: loop
    --     fetch _cur into _task_id, _host;
    --     if _done then
    --         leave read_loop;
    --     end if;

    --     set _ok = false;

    --     begin
    --         declare exit handler for sqlexception begin
    --             rollback;
    --         end;

    --         declare exit handler for sqlwarning begin
    --             rollback;
    --         end;

    --         start transaction;
    --             insert into worker (worker_id, task_id, host)
    --             values (_worker_id, _task_id, _host);
    --         commit;

    --         set _ok = true;
    --     end;

    --     -- set _ok = lock_task(_worker_id, _task_id, _host);

    --     if _ok then
    --         leave read_loop;
    --     end if;
    -- end loop;

    -- close _cur;

    -- if _ok then
    --     update task set status = 'STARTED' where task_id = _task_id;
    --     insert into task_log (task_id, worker_id) values (_task_id, _worker_id);
    --     select _task_id as task_id, _host as host;
    -- end if;
end $$

create function lock_task(_worker_id varchar(255), _task_id int, _host text)
    returns boolean
begin
    -- TODO: default value should be false
    declare _ok boolean default true;

    -- duplicate key
    declare continue handler for sqlstate '23000' set _ok = false;

    -- deadlock
    -- declare continue handler for sqlstate '40001' set _ok = false;

    insert into worker (worker_id, task_id, host)
    values (_worker_id, _task_id, _host);

    insert into task_log (worker_id, task_id, status)
    values (_worker_id, _task_id, 'STARTED');

    return _ok;
end$$

delimiter ; $$
