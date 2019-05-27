select id_tienda, id_tejido, id_provincia, id_pais, id_modelo, id_mes, id_familia, count(*)
   from 
(
   SELECT omes.id_mes, 
          opai.id_pais, 
          opro.id_provincia,
          otie.id_tienda,
          ofam.id_familia,
          otej.id_tejido,
          omod.id_modelo,
          oven.unidades,
          oven.pvp_unidad * oven.unidades
     FROM origen.ventas oven
        INNER JOIN origen.fechas ofec ON oven.id_fecha = ofec.id_fecha
        INNER JOIN origen.meses omes ON substr(oven.id_fecha,4,2) = omes.num_mes
        INNER JOIN origen.modelos omod ON oven.id_modelo = omod.id_modelo
        INNER JOIN origen.familias ofam ON omod.id_familia = ofam.id_familia
        INNER JOIN origen.tejidos otej ON omod.id_tejido = otej.id_tejido
        INNER JOIN origen.tiendas otie ON oven.id_tienda = otie.id_tienda
        INNER JOIN origen.provincias opro ON otie.id_provincia = opro.id_provincia
        INNER JOIN origen.paises opai ON opro.id_pais = opai.id_pais
        INNER JOIN origen.tienda_zona otiz ON otie.id_tienda = otiz.id_tienda
        INNER JOIN origen.zonas_influencia ozoi ON otiz.id_zona_influencia = ozoi.id_zona_influencia)
 group by (id_tienda, id_tejido, id_provincia, id_pais, id_modelo, id_mes, id_familia)       
 order by count(*) desc
 
