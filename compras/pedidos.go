package compras

import (
	"database/sql"
	"fmt"
	"FDBtoFDB/conexao"
)

func Cadped(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM icadped")
	cnx_dest.Exec("DELETE FROM cadped")
	Cnx, _ := conexao.Conexao()

	insert, err := cnx_dest.Prepare(`Insert into cadped (numped, num, ano, datped, codif, total, entrou, codccusto, id_cadped, empresa, numlic, nempg) values (?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := Cnx.Query(`select numped, num, ano, datped, codif, total, entrou, codccusto, id_cadped, empresa, numlic, nempg from cadped where ano <= ?`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx, _ := cnx_dest.Begin()
	for rows.Next() {
		var numped, num, ano, codif, codccusto, id_cadped, empresa, numlic, nempg, entrou sql.NullString
		var datped sql.NullTime
		var total sql.NullFloat64
		err = rows.Scan(&numped, &num, &ano, &datped, &codif, &total, &entrou, &codccusto, &id_cadped, &empresa, &numlic, &nempg)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(numped, num, ano, datped, codif, total, entrou, codccusto, id_cadped, empresa, numlic, nempg)
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

func Icadped(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM icadped")
	Cnx, _ := conexao.Conexao()

	insert, err := cnx_dest.Prepare(`Insert into icadped (numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped) values (?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := Cnx.Query(`select numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped from icadped where id_cadped in (select id_cadped from cadped where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx, _ := cnx_dest.Begin()
	for rows.Next() {
		var numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped sql.NullString
		err = rows.Scan(&numped, &item, &cadpro, &qtd, &prcunt, &prctot, &codccusto, &id_cadped)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped)
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

func LimpaPedidos(cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM icadped")
	cnx_dest.Exec("DELETE FROM cadped")
}