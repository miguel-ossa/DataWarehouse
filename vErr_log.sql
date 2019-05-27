create or replace view destino.err_log as
select l.id, fecha_inicio, fecha_fin, duracion, procedimiento, paso, texto_err
    from destino.etl_log l
    right join destino.etl_log_err e on e.id = l.id
    order by l.id

