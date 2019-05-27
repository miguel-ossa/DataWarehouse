create or replace procedure CargarDimTiendas is
begin

MERGE INTO destino.dim_tiendas des_tie
USING (
   SELECT tiendas.id_tienda 
          ,tienda
          ,tipo_tienda
          ,id_provincia
          ,vendedor_003
          ,MAX(zona_influencia) zona_influencia
   FROM 
        origen.tiendas
        INNER JOIN origen.tienda_zona ON (tiendas.id_tienda=tienda_zona.id_tienda)
        INNER JOIN origen.zonas_influencia ON (zonas_influencia.id_zona_influencia=tienda_zona.id_zona_influencia)
   GROUP BY
         tiendas.id_tienda
         ,tienda
         ,tipo_tienda
         ,id_provincia
         ,vendedor_003
   HAVING 
          COUNT(*)=1) ori_tie
  ON (des_tie.id_tienda = ori_tie.id_tienda)
WHEN MATCHED THEN
   UPDATE SET zona_influencia=substr(ori_tie.zona_influencia,1,25)
              ,tienda = ori_tie.tienda
              ,tipo_tienda = ori_tie.tipo_tienda
              ,id_provincia = ori_tie.id_provincia
              ,vendedor_003 = ori_tie.vendedor_003
              ,fecha_modificacion = SYSDATE
WHEN NOT MATCHED THEN
   INSERT (id_tienda 
          ,tienda
          ,tipo_tienda
          ,id_provincia
          ,vendedor_003
          ,zona_influencia
          ,fecha_alta)
      VALUES 
          (ori_tie.id_tienda
          ,ori_tie.tienda
          ,ori_tie.tipo_tienda
          ,ori_tie.id_provincia
          ,ori_tie.vendedor_003
          ,substr(ori_tie.zona_influencia, 1, 25)
          ,SYSDATE);

--Bajas
UPDATE
  (SELECT
        dtie.fecha_baja fecha_baja
      FROM
        destino.dim_tiendas dtie
        LEFT JOIN origen.tiendas otie ON otie.id_tienda = dtie.id_tienda
      WHERE otie.id_tienda IS NULL
  )
  SET fecha_baja = SYSDATE;
            
COMMIT;          
  
end CargarDimTiendas;
/
