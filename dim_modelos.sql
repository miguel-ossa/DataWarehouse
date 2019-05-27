-- Author  : MOSSA
-- Created : 14/12/2004
-- Purpose : Carga de DESTINO.DIM_MODELOS
BEGIN

--Modificaciones
UPDATE 
  (SELECT 
        dmod.modelo               old_modelo,
        dmod.temporada            old_temporada,
        dmod.id_familia           old_id_familia,
        dmod.id_tejido            old_id_tejido,
        dmod.fecha_modificacion   fecha_modificacion,
        --
        omod.modelo               new_modelo,
        omod.temporada            new_temporada,
        omod.id_familia           new_id_familia,
        omod.id_tejido            new_id_tejido
      FROM
        origen.modelos omod
        LEFT JOIN destino.dim_modelos dmod ON dmod.id_modelo = omod.id_modelo
      WHERE dmod.id_modelo IS NOT NULL AND
            (dmod.modelo <> omod.modelo OR
             dmod.temporada <> omod.temporada OR
             dmod.id_familia <> omod.id_familia OR
             dmod.id_tejido <> omod.id_tejido)
  )
  SET old_modelo = new_modelo,
      old_temporada = new_temporada,
      old_id_familia = new_id_familia,
      old_id_tejido = new_id_tejido,
      fecha_modificacion = SYSDATE;

--Altas      
INSERT INTO destino.dim_modelos 
  (id_modelo
  ,modelo
  ,id_familia
  ,id_tejido
  ,fecha_alta)
  (SELECT 
        omod.id_modelo,
        omod.modelo,
        omod.id_familia,
        omod.id_tejido,
        SYSDATE
      FROM
        origen.modelos omod
        LEFT JOIN destino.dim_modelos dmod ON dmod.id_modelo = omod.id_modelo
       WHERE dmod.id_modelo IS NULL);

--Bajas       
UPDATE
  (SELECT
        dmod.fecha_baja fecha_baja
      FROM 
        destino.dim_modelos dmod
        LEFT JOIN origen.modelos omod ON omod.id_modelo = dmod.id_modelo
      WHERE omod.id_modelo IS NULL   
  )
  SET fecha_baja = SYSDATE;
           
COMMIT;

END;
