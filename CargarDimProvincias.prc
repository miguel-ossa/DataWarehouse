create or replace procedure CargarDimProvincias is

begin

--Modificaciones
UPDATE 
  (SELECT 
        dprov.desc_provincia old_desc_provincia
        ,dprov.id_pais        old_id_pais
        ,fecha_modificacion   fecha_modificacion
        --
        ,oprov.desc_provincia new_desc_provincia
        ,oprov.id_pais        new_id_pais
      FROM
        origen.provincias oprov
        LEFT JOIN destino.dim_provincias dprov ON dprov.id_provincia = oprov.id_provincia
      WHERE dprov.id_provincia IS NOT NULL AND
            (dprov.desc_provincia <> oprov.desc_provincia OR
            dprov.id_pais <> oprov.id_pais)
  )
  SET old_desc_provincia    = new_desc_provincia
      ,old_id_pais          = new_id_pais
      ,fecha_modificacion   = SYSDATE;

--Altas           
INSERT INTO destino.dim_provincias 
  (id_pais
  ,id_provincia
  ,desc_provincia
  ,fecha_alta)
  (SELECT oprov.id_pais, oprov.id_provincia, oprov.desc_provincia, SYSDATE
       FROM
           origen.provincias oprov
           LEFT JOIN destino.dim_provincias dprov ON dprov.id_provincia = oprov.id_provincia
       WHERE dprov.id_provincia IS NULL);

--Bajas
UPDATE
  (SELECT
        dprov.fecha_baja fecha_baja
      FROM
        destino.dim_provincias dprov
        LEFT JOIN origen.provincias oprov ON oprov.id_provincia = dprov.id_provincia
      WHERE oprov.id_provincia IS NULL
  )
  SET fecha_baja = SYSDATE;
              
COMMIT;

end CargarDimProvincias;
/
