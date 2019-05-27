/* tiendas inexistentes */
select distinct codigo_003                 tienda
                ,nombre_003                nombre
                ,substr(t.codigo_003,5,4)  substr
  from pau.fich333_t f
    left join pau.fich003 t 
      on substr(t.codigo_003,5,4) = f.almacen_333
  where t.codigo_003 is null
  order by substr

/* totales sin lineas */  
select c.tipo_cliente_003   tipo
         ,CASE WHEN c.tipo_cliente_003 = 'F' then 'Franquicia'
               WHEN c.tipo_cliente_003 = 'D' then 'Deposito'
               WHEN c.tipo_cliente_003 = 'N' then 'Normal'
               WHEN c.tipo_cliente_003 = 'A' then 'Almacen'
               ELSE 'Otros'
          END tipo_cliente
          ,count(*) total
    from pau.fich333_t t
      left join pau.fich333_l l
        on ( l.almacen_333         = t.almacen_333 and
             l.numero_333          = t.numero_333  and
             l.fecha_operacion_333 = t.fecha_operacion_333 )
      left join pau.fich003 c 
        on substr(c.codigo_003,5,4) = t.almacen_333
    where l.almacen_333 is null
    group by c.tipo_cliente_003
/* modelos inexistentes */
select modelo_333
      ,talla_333
      ,color_333
  from pau.fich333_l
    left join pau.fich010_mtc on
      modelo_333 = modelo_010 and
      talla_333 = talla_010 and
      color_333 = color_010 and
      modelo_333 <> '03000000'
  where rownum < 15000 and modelo_010 is null

create table pau.fich010_mtc
(
  modelo_010
  ,talla_010
  ,color_010
  ,nombre_010
) as
(select distinct trim(parte_1_mod_010)||trim(parte_2_mod_010)
        ,talla_010
        ,color_010
        ,nombre_018
    from pau.fich010
      left join pau.fich018 on 
        trim(parte_1_mod_018) = trim(parte_1_mod_010) and
        trim(parte_2_mod_018) = trim(parte_2_mod_010))

        
SELECT * FROM DWH.DWH_AGR_VENTAS