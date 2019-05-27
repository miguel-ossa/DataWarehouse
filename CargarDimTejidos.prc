create or replace procedure CargarDimTejidos is

begin

--Actualizaciones
UPDATE 
  (SELECT 
        dtej.tejido               old_tejido
        ,dtej.fecha_modificacion  fecha_modificacion
        ,otej.tejido              new_tejido
      FROM
        origen.tejidos otej
        LEFT JOIN destino.dim_tejidos dtej ON dtej.id_tejido = otej.id_tejido
      WHERE dtej.id_tejido IS NOT NULL AND
            dtej.tejido <> otej.tejido)
  SET old_tejido = new_tejido
      ,fecha_modificacion = SYSDATE;

--Altas           
INSERT INTO destino.dim_tejidos
  (id_tejido
  ,tejido
  ,fecha_alta)
  (SELECT 
        otej.id_tejido
        ,otej.tejido
        ,SYSDATE
      FROM
        origen.tejidos otej
        LEFT JOIN destino.dim_tejidos dtej ON dtej.id_tejido = otej.id_tejido
       WHERE dtej.id_tejido IS NULL);

--Bajas
UPDATE
  (SELECT 
        dtej.fecha_baja fecha_baja
      FROM
        destino.dim_tejidos dtej
        LEFT JOIN origen.tejidos otej ON otej.id_tejido = dtej.id_tejido
      WHERE otej.id_tejido IS NULL
  )
  SET fecha_baja = SYSDATE;
            
COMMIT;

end CargarDimTejidos;
/
