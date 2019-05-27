select num_lineas
      ,count(*) 
   from 
   (
    select t.almacen_333
          ,t.numero_333
          ,t.fecha_operacion_333
          ,count(*)               num_lineas
        from pau.fich333_t t
          left join pau.fich333_l l
              on ( l.almacen_333         = t.almacen_333 and
                   l.numero_333          = t.numero_333  and
                   l.fecha_operacion_333 = t.fecha_operacion_333 )
        group by t.almacen_333,t.numero_333,t.fecha_operacion_333
    ) 
    group by num_lineas
    order by num_lineas
