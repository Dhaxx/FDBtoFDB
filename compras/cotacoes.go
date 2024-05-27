package compras

import (
	"database/sql"
	"fmt"
	"FDBtoFDB/conexao"
)

func Cadorc(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM ICADORC")
	cnx_dest.Exec("DELETE FROM CADORC")
	Cnx, _ := conexao.Conexao()
	rows, err := Cnx.Query(`select id_cadorc, num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela, empresa, registropreco, numlic, proclic from cadorc where ano = ?`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	insert, err := cnx_dest.Prepare("insert into cadorc (id_cadorc, num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela, empresa, registropreco, numlic, proclic) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		fmt.Println(err)
		return
	}

	tx, _ := cnx_dest.Begin()
	for rows.Next() {
		var id_cadorc, num, ano, numorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela, empresa, registropreco, numlic, proclic sql.NullString
		var dtorc sql.NullTime
		err = rows.Scan(&id_cadorc, &num, &ano, &numorc, &dtorc, &descr, &prioridade, &obs, &status, &liberado, &codccusto, &liberado_tela, &empresa, &registropreco, &numlic, &proclic)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(id_cadorc, num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela, empresa, registropreco, numlic, proclic)
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

func Icadorc(ano int, cnx_dest *sql.DB) {
	Cnx, _ := conexao.Conexao()
	rows, err := Cnx.Query(`select numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc from icadorc where id_cadorc in (select id_cadorc from cadorc where ano = ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	insert, err := cnx_dest.Prepare("insert into icadorc (numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc) VALUES (?,?,?,?,?,?,?,?,?)")
	if err != nil {
		fmt.Println(err)
		return
	}

	tx, _ := cnx_dest.Begin()
	for rows.Next() {
		var numorc, item, cadpro, itemorc, codccusto, itemorc_ag, id_cadorc sql.NullString
		var qtd, valor sql.NullFloat64
		err = rows.Scan(&numorc, &item, &cadpro, &qtd, &valor, &itemorc, &codccusto, &itemorc_ag, &id_cadorc)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc)
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

func Fcadorc(ano int, cnx_dest *sql.DB) {
	insert, err := cnx_dest.Prepare(`insert into fcadorc (numorc,codif, nome, valorc, id_cadorc) values (?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	Cnx, _ := conexao.Conexao()

	rows, err := Cnx.Query(`select numorc, codif, nome, valorc, id_cadorc from fcadorc where id_cadorc in (select id_cadorc from cadorc where ano = ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx, _ := cnx_dest.Begin()
	for rows.Next() {
		var numorc, codif, nome, id_cadorc sql.NullString
		var valorc sql.NullFloat64
		err = rows.Scan(&numorc, &codif, &nome, &valorc, &id_cadorc)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(numorc, codif, nome, valorc, id_cadorc)
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

func Vcadorc(ano int, cnx_dest *sql.DB) {
	insert, err := cnx_dest.Prepare(`insert into vcadorc(numorc,codif,vlruni,vlrtot,item,id_cadorc,classe,ganhou,vlrganhou) values(?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	Cnx, _ := conexao.Conexao()

	rows, err := Cnx.Query(`select numorc, codif, vlruni, vlrtot, item, id_cadorc, classe, ganhou, vlrganhou from vcadorc where id_cadorc in (select id_cadorc from cadorc where ano = ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx, _ := cnx_dest.Begin()
	for rows.Next() {
		var numorc, codif, item, id_cadorc, classe, ganhou sql.NullString
		var vlruni, vlrtot, vlrganhou sql.NullFloat64
		err = rows.Scan(&numorc, &codif, &vlruni, &vlrtot, &item, &id_cadorc, &classe, &ganhou, &vlrganhou)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(numorc, codif, vlruni, vlrtot, item, id_cadorc, classe, ganhou, vlrganhou)
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

func Icadorc_cot(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM ICADORC_COT")
	Cnx, _ := conexao.Conexao()
	rows, err := Cnx.Query(`select numorc, item, codif, tipo, qtd, valunt, valtot, qtdped, id_cadorc, flg_aceito, flg_alt_user from icadorc_cot where id_cadorc in (select id_cadorc from cadorc where ano = ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	insert, err := cnx_dest.Prepare(`insert into icadorc_cot (numorc, item, codif, tipo, qtd, valunt, valtot, qtdped, id_cadorc, flg_aceito, flg_alt_user) VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx, _ := cnx_dest.Begin()
	for rows.Next() {
		var numorc, item, codif, tipo, id_cadorc, flg_aceito, flg_alt_user sql.NullString
		var qtd, valunt, valtot, qtdped sql.NullFloat64
		err = rows.Scan(&numorc, &item, &codif, &tipo, &qtd, &valunt, &valtot, &qtdped, &id_cadorc, &flg_aceito, &flg_alt_user)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(numorc, item, codif, tipo, qtd, valunt, valtot, qtdped, id_cadorc, flg_aceito, flg_alt_user)
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
