create or replace procedure CargarDimFamilias is

begin

--Actualizaciones
UPDATE 
  (SELECT 
        dfam.familia              old_familia
        ,dfam.fecha_modificacion  fecha_modificacion
        ,ofam.familia             new_familia
      FROM
        origen.familias ofam
        LEFT JOIN destino.dim_familias dfam ON dfam.id_familia = ofam.id_familia
      WHERE dfam.id_familia IS NOT NULL AND
            dfam.familia <> ofam.familia)
  SET old_familia = new_familia,
      fecha_modificacion = SYSDATE;

--Altas           
INSERT INTO destino.dim_familias
  (id_familia
  ,familia
  ,fecha_alta)
  (SELECT 
        ofam.id_familia, ofam.familia, SYSDATE
      FROM
        origen.familias ofam
        LEFT JOIN destino.dim_familias dfam ON dfam.id_familia = ofam.id_familia
       WHERE dfam.id_familia IS NULL);

--Bajas
UPDATE
  (SELECT
        dfam.fecha_baja fecha_baja
      FROM
        destino.dim_familias dfam
        LEFT JOIN origen.familias ofam ON ofam.id_familia = dfam.id_familia
      WHERE ofam.id_familia IS NULL
  )
  SET fecha_baja = SYSDATE;
            
COMMIT;

end CargarDimFamilias;
/
