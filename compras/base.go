package compras

import (
		"fmt"
		"database/sql"
		"FDBtoFDB/conexao"
)

func Cadunimedida(cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM CADUNIMEDIDA")
	
	rows, err := conexao.Cnx.Query("SELECT sigla, descricao FROM CADUNIMEDIDA")
	if err != nil {
		fmt.Println(err)
		return
	}
	
	insert, err := cnx_dest.Prepare("INSERT INTO CADUNIMEDIDA(sigla, descricao) VALUES(?,?)")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer insert.Close()

	for rows.Next() {
		var sigla, descricao string
		err = rows.Scan(&sigla, &descricao)
		if err != nil {
			fmt.Println(err)
			return
		}
		_, err = insert.Exec(sigla, descricao)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func GrupoSubgrupo(cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM CADGRUPO")
	cnx_dest.Exec("DELETE FROM CADSUBGR")

	grupos, err := conexao.Cnx.Query("SELECT grupo, nome, ocultar FROM CADGRUPO")
	if err != nil {
		fmt.Println(err)
		return
	}

	subgrupos, err := conexao.Cnx.Query("SELECT grupo, subgrupo, nome, ocultar FROM CADSUBGR")
	if err != nil {
		fmt.Println(err)
		return
	}

	insertGrupo, err := cnx_dest.Prepare("INSERT INTO CADGRUPO(grupo, nome, ocultar) VALUES(?,?,?)")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer insertGrupo.Close()

	insertSubgrupo, err := cnx_dest.Prepare("INSERT INTO CADSUBGR(grupo, subgrupo, nome, ocultar) VALUES(?,?,?,?)")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer insertSubgrupo.Close()

	for grupos.Next() {
		var grupo, nome, ocultar string
		err = grupos.Scan(&grupo, &nome, &ocultar)
		if err != nil {
			fmt.Println(err)
			return
		}
		_, err = insertGrupo.Exec(grupo, nome, ocultar)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	for subgrupos.Next() {
		var grupo, subgrupo, nome, ocultar string
		err = subgrupos.Scan(&grupo, &subgrupo, &nome, &ocultar)
		if err != nil {
			fmt.Println(err)
			return
		}
		_, err = insertSubgrupo.Exec(grupo, subgrupo, nome, ocultar)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func Cadest(cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM CADEST")
	rows, err := conexao.Cnx.Query(`select cadpro, grupo, subgrupo, codigo, disc1, tipopro, unid1, discr1, codreduz, ocultar, substring(dtainsere from 1 for 10) dtainsere from cadest`)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	insert, err := cnx_dest.Prepare("INSERT INTO CADEST(cadpro, grupo, subgrupo, codigo, disc1, tipopro, unid1, discr1, codreduz, ocultar, dtainsere) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var cadpro, grupo, subgrupo, codigo, disc1, tipopro, unid1, discr1, codreduz, ocultar, dtainsere string
		err = rows.Scan(&cadpro, &grupo, &subgrupo, &codigo, &disc1, &tipopro, &unid1, &discr1, &codreduz, &ocultar, &dtainsere)
		if err != nil {
			fmt.Println(err)
			return
		}
		_, err = insert.Exec(cadpro, grupo, subgrupo, codigo, disc1, tipopro, unid1, discr1, codreduz, ocultar, dtainsere)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	err = tx.Commit()	
	if err != nil {
		fmt.Println(err)
		return
	}
}

func Destino(cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM DESTINO")
	rows, err := conexao.Cnx.Query(`select COD, DESTI, EMPRESA from destino`)
	if err != nil {
		fmt.Println(err)
		return
	}

	insert, err := cnx_dest.Prepare("insert into destino (COD, DESTI, EMPRESA) VALUES(?,?,?)")
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var cod, desti, empresa string
		err = rows.Scan(&cod, &desti, &empresa)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(cod, desti, empresa)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func CentroCusto(cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM CENTROCUSTO")
	rows, err := conexao.Cnx.Query(`select poder, orgao, destino, ccusto, descr, obs, placa, codccusto, empresa, unidade, ocultar from centrocusto`)
	if err != nil {
		fmt.Println(err)
		return
	}

	insert, err := cnx_dest.Prepare("insert into centrocusto (poder, orgao, destino, ccusto, descr, obs, placa, codccusto, empresa, unidade, ocultar) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var poder, orgao, destino, ccusto, descr, obs, placa, codccusto, empresa, unidade, ocultar sql.NullString
		err = rows.Scan(&poder, &orgao, &destino, &ccusto, &descr, &obs, &placa, &codccusto, &empresa, &unidade, &ocultar)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(poder, orgao, destino, ccusto, descr, obs, placa, codccusto, empresa, unidade, ocultar)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return
	}
}