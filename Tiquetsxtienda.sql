select tot.almacen_333          tienda
      ,tot.numero_333           tiquet
      ,tot.fecha_operacion_333  fecha
      ,count(*)                 repes
    from pau.fich333_t tot
      left join pau.fich003 tie 
          on tot.almacen_333 = to_number(ltrim(to_char(tie.codigo_003), '0001'))
      left join pau.fich063 pai on tie.codigo_pais_003 = pai.pais_063
    where trim(pai.nombre_063) <> 'POLONIA'
    group by tot.almacen_333
             ,tot.numero_333
             ,tot.fecha_operacion_333
    having count(*)<>1
    order by count(*) desc
    