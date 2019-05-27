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
        