-- Author  : MOSSA
-- Created : 14/12/2004
-- Purpose : Carga de DESTINO.FACT_VENTAS
BEGIN

SELECT
    dvta.unidades     old_unidades 
    ,dvta.pvp_total   old_pvp_total
    --
    ,ovta.unidades     new_unidades
    ,ovta.pvp_unidad  new_pvp_unidad
  FROM
    destino.fact_ventas dvta
    INNER JOIN origen.meses omes ON dvta.id_mes = omes.id_mes
    INNER JOIN origen.modelos omod ON oven.id_modelo = omod.id_modelo
    INNER JOIN origen.familias ofam ON omod.id_familia = ofam.id_familia
    LEFT JOIN origen.tejidos otej ON omod.id_tejido = otej.id_tejido
    INNER JOIN origen.tiendas otie ON oven.id_tienda = otie.id_tienda
    INNER JOIN origen.provincias opro ON otie.id_provincia = opro.id_provincia
    INNER JOIN origen.paises opai ON opro.id_pais = opai.id_pais
  
INSERT INTO destino.fact_ventas 
  (id_mes, id_pais, id_provincia, id_tienda, id_familia, id_tejido, id_modelo, unidades, pvp_total)
   SELECT 
          omes.id_mes, 
          opai.id_pais, 
          opro.id_provincia,
          otie.id_tienda,
          ofam.id_familia,
          nvl(otej.id_tejido,'0'),
          omod.id_modelo,
          sum(oven.unidades),
          sum(oven.unidades * oven.pvp_unidad)
     FROM 
          origen.ventas oven   
          INNER JOIN origen.fechas ofec ON oven.id_fecha = ofec.id_fecha
          INNER JOIN origen.meses omes ON ofec.id_mes = omes.id_mes
          INNER JOIN origen.modelos omod ON oven.id_modelo = omod.id_modelo
          INNER JOIN origen.familias ofam ON omod.id_familia = ofam.id_familia
          LEFT JOIN origen.tejidos otej ON omod.id_tejido = otej.id_tejido
          INNER JOIN origen.tiendas otie ON oven.id_tienda = otie.id_tienda
          INNER JOIN origen.provincias opro ON otie.id_provincia = opro.id_provincia
          INNER JOIN origen.paises opai ON opro.id_pais = opai.id_pais
     GROUP BY 
           omes.id_mes,
           opai.id_pais, 
           opro.id_provincia,
           otie.id_tienda,
           ofam.id_familia,
           NVL(otej.id_tejido,'0'),
           omod.id_modelo;

COMMIT;

END;
