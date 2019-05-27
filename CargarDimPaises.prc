create or replace procedure CargarDimPaises is

begin

--Modificaciones
UPDATE
  (SELECT
        dpai.desc_pais      old_desc_pais
        ,fecha_modificacion fecha_modificacion
        ,opai.desc_pais      new_desc_pais
      FROM
        origen.paises opai
        LEFT JOIN destino.dim_paises dpai ON dpai.id_pais = opai.id_pais
      WHERE dpai.id_pais IS NOT NULL AND
            dpai.desc_pais <> opai.desc_pais)
    SET old_desc_pais = new_desc_pais,
        fecha_modificacion = SYSDATE;

--Altas    
INSERT INTO destino.dim_paises
  (id_pais
  ,desc_pais
  ,fecha_alta)
   (SELECT
          opai.id_pais
          ,opai.desc_pais
          ,SYSDATE
     FROM 
          origen.paises opai
          LEFT JOIN destino.dim_paises dpai ON dpai.id_pais = opai.id_pais
     WHERE dpai.id_pais IS NULL); 

--Bajas
UPDATE
  (SELECT 
        dpai.fecha_baja fecha_baja
      FROM
        destino.dim_paises dpai
        LEFT JOIN origen.paises opai ON opai.id_pais = dpai.id_pais
      WHERE opai.id_pais IS NULL
  )
  SET fecha_baja = SYSDATE;    
  
COMMIT;

end CargarDimPaises;
/
