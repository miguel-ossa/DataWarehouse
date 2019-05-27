create or replace procedure CargarDimMeses is

begin

--Modificaciones
UPDATE
  (SELECT
        dmes.anyo           old_anyo
        ,dmes.num_mes       old_num_mes
        ,dmes.mes           old_mes
        ,fecha_modificacion fecha_modificacion
        --
        ,omes.anyo          new_anyo
        ,omes.num_mes       new_num_mes
        ,omes.mes           new_mes
    FROM
        origen.meses omes
        LEFT JOIN destino.dim_meses dmes ON dmes.id_mes = omes.id_mes
    WHERE dmes.id_mes IS NOT NULL AND
          (dmes.anyo <> omes.anyo OR
           dmes.mes <> omes.mes OR
           dmes.num_mes <> omes.num_mes)
  )
  SET old_anyo    = new_anyo
      ,old_num_mes = new_num_mes
      ,old_mes     = new_mes
      ,fecha_modificacion = SYSDATE;

--Altas      
INSERT INTO destino.dim_meses
  (id_mes
  ,anyo
  ,num_mes
  ,mes
  ,fecha_alta)
  (SELECT
        omes.id_mes
        ,omes.anyo
        ,omes.num_mes
        ,omes.mes
        ,SYSDATE
     FROM 
        origen.meses omes
        LEFT JOIN destino.dim_meses dmes ON dmes.id_mes = omes.id_mes
     WHERE dmes.id_mes IS NULL);
     
--Bajas
UPDATE
  (SELECT
        dmes.fecha_baja fecha_baja
      FROM
        destino.dim_meses dmes
        LEFT JOIN origen.meses omes ON omes.id_mes = dmes.id_mes
      WHERE omes.id_mes IS NULL
  )
  SET fecha_baja = SYSDATE;     

COMMIT;

end CargarDimMeses;
/
