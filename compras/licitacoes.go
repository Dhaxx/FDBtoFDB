package compras

import (
	"fmt"
	"database/sql"
	"FDBtoFDB/conexao"
)

func Cadlic(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM CADLIC")

	insert, err := cnx_dest.Prepare(`insert into cadlic (numpro,
					datae,
					dtpub,
					dtenc,
					horabe,
					discr,
					discr7,
					modlic,
					dthom,
					dtadj,
					comp,
					numero,
					ano,
					registropreco,
					ctlance,
					obra,
					proclic,
					numlic,
					liberacompra,
					microempresa,
					licnova,
					tlance,
					mult_entidade,
					processo_ano,
					LEI_INVERTFASESTCE,
					codmod,
					empresa,
					valor,
					detalhe,
					anomod,
					processo,
					codtce,
					dtenvio_tce,
					enviotce)
				VALUES(?,?,?,?,?,
					?,?,?,?,?,
					?,?,?,?,?,
					?,?,?,?,?,
					?,?,?,?,?,
					?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := conexao.Cnx.Query(`select numpro,
										substring(datae from 1 for 10) datae,
										substring(dtpub from 1 for 10) dtpub,
										substring(dtenc from 1 for 10) dtenc,
										horabe,
										discr,
										discr7,
										modlic,
										substring(dthom from 1 for 10) dthom,
										substring(dtadj from 1 for 10) dtadj,
										comp,
										numero,
										ano,
										registropreco,
										ctlance,
										obra,
										proclic,
										numlic,
										liberacompra,
										microempresa,
										licnova,
										tlance,
										mult_entidade,
										processo_ano,
										LEI_INVERTFASESTCE,
										codmod,
										empresa,
										valor,
										detalhe,
										anomod,
										processo,
										codtce,
										dtenvio_tce,
										enviotce from cadlic where ano <= ?`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var numpro, datae, dtpub, dtenc, horabe, discr, discr7, modlic, dthom, dtadj, comp, numero, ano, registropreco, ctlance, obra, proclic, numlic, liberacompra, microempresa, licnova, tlance, mult_entidade, processo_ano, LEI_INVERTFASESTCE, codmod, empresa, valor, detalhe, anomod, processo, codtce, dtenvio_tce, enviotce sql.NullString
		err = rows.Scan(&numpro, &datae, &dtpub, &dtenc, &horabe, &discr, &discr7, &modlic, &dthom, &dtadj, &comp, &numero, &ano, &registropreco, &ctlance, &obra, &proclic, &numlic, &liberacompra, &microempresa, &licnova, &tlance, &mult_entidade, &processo_ano, &LEI_INVERTFASESTCE, &codmod, &empresa, &valor, &detalhe, &anomod, &processo, &codtce, &dtenvio_tce, &enviotce)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(numpro, datae, dtpub, dtenc, horabe, discr, discr7, modlic, dthom, dtadj, comp, numero, ano, registropreco, ctlance, obra, proclic, numlic, liberacompra, microempresa, licnova, tlance, mult_entidade, processo_ano, LEI_INVERTFASESTCE, codmod, empresa, valor, detalhe, anomod, processo, codtce, dtenvio_tce, enviotce)
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

func Cadprolic(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM CADPROLIC")

	insert, err := cnx_dest.Prepare(`insert into cadprolic (item, item_mask, numorc, cadpro, quan1, vamed1, vatomed1, codccusto,
									 reduz, numlic, microempresa, tlance, item_ag, id_cadorc) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := conexao.Cnx.Query(`select item, item_mask, numorc, cadpro, quan1, vamed1, vatomed1, codccusto, reduz, numlic, microempresa, tlance, item_ag, id_cadorc from cadprolic where numlic in (select numlic from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var item, item_mask, numorc, cadpro, quan1, vamed1, vatomed1, codccusto, reduz, numlic, microempresa, tlance, item_ag, id_cadorc sql.NullString
		err = rows.Scan(&item, &item_mask, &numorc, &cadpro, &quan1, &vamed1, &vatomed1, &codccusto, &reduz, &numlic, &microempresa, &tlance, &item_ag, &id_cadorc)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(item, item_mask, numorc, cadpro, quan1, vamed1, vatomed1, codccusto, reduz, numlic, microempresa, tlance, item_ag, id_cadorc)
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

func Cadprolic_detalhe(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec("DELETE FROM cadprolic_detalhe")

	insert, err := cnx_dest.Prepare(`insert into cadprolic_detalhe (NUMLIC,item,CADPRO,quan1,VAMED1,VATOMED1,marca,CODCCUSTO,ITEM_CADPROLIC) values (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := conexao.Cnx.Query(`select NUMLIC,item,CADPRO,quan1,VAMED1,VATOMED1,marca,CODCCUSTO,ITEM_CADPROLIC from cadprolic_detalhe where NUMLIC in (select numlic from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var NUMLIC, item, CADPRO, quan1, VAMED1, VATOMED1, marca, CODCCUSTO, ITEM_CADPROLIC sql.NullString
		err = rows.Scan(&NUMLIC, &item, &CADPRO, &quan1, &VAMED1, &VATOMED1, &marca, &CODCCUSTO, &ITEM_CADPROLIC)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(NUMLIC, item, CADPRO, quan1, VAMED1, VATOMED1, marca, CODCCUSTO, ITEM_CADPROLIC)
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

func ProlicProlics(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec(`DELETE FROM PROLICS`)
    cnx_dest.Exec(`DELETE FROM PROLIC`)

	insert_prolic, err := cnx_dest.Prepare(`insert into PROLIC (codif, nome, status, numlic) VALUES (?, ?, ?, ?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	insert_prolics, err := cnx_dest.Prepare(`insert into prolics (sessao, codif, status, representante, numlic, usa_preferencia) values (?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	prolic, err := conexao.Cnx.Query(`select codif, nome, status, numlic from prolic where numlic in (select numlic from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	prolics, err := conexao.Cnx.Query(`select sessao, codif, status, representante, numlic, usa_preferencia from prolics where numlic in (select numlic from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx_prolic,_ := cnx_dest.Begin()
	for prolic.Next() {
		var codif, nome, status, numlic sql.NullString
		err = prolic.Scan(&codif, &nome, &status, &numlic)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert_prolic.Exec(codif, nome, status, numlic)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	for prolics.Next() {
		var sessao, codif, status, representante, numlic, usa_preferencia sql.NullString
		err = prolics.Scan(&sessao, &codif, &status, &representante, &numlic, &usa_preferencia)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert_prolics.Exec(sessao, codif, status, representante, numlic, usa_preferencia)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	err = tx_prolic.Commit()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func Cadpro_status(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec(`DELETE FROM CADPRO_STATUS`)

	insert, err := cnx_dest.Prepare(`insert into CADPRO_STATUS (numlic, sessao, itemp, item) VALUES (?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := conexao.Cnx.Query(`select numlic, sessao, itemp, item from cadpro_status where numlic in (select numlic from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var numlic, sessao, itemp, item sql.NullString
		err = rows.Scan(&numlic, &sessao, &itemp, &item)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(numlic, sessao, itemp, item)
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

func Cadpro_proposta(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec(`DELETE FROM CADPRO_PROPOSTA`)

	insert, err := cnx_dest.Prepare(`insert into cadpro_proposta (codif, sessao, numlic, itemp, item, quan1, vaun1, vato1, status, marca, subem) values (?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := conexao.Cnx.Query(`select codif, sessao, numlic, itemp, item, quan1, vaun1, vato1, status, marca, subem from cadpro_proposta where numlic in (select numlic from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var codif, sessao, numlic, itemp, item, quan1, vaun1, vato1, status, marca, subem sql.NullString
		err = rows.Scan(&codif, &sessao, &numlic, &itemp, &item, &quan1, &vaun1, &vato1, &status, &marca, &subem)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(codif, sessao, numlic, itemp, item, quan1, vaun1, vato1, status, marca, subem)
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

func Cadpro_lance(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec(`DELETE FROM CADPRO_LANCE`)

	insert, err := cnx_dest.Prepare(`insert into cadpro_lance (sessao, rodada, codif, itemp, vaunl, vatol, status, subem, numlic) values (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := conexao.Cnx.Query(`select sessao, rodada, codif, itemp, vaunl, vatol, status, subem, numlic from cadpro_lance where numlic in (select numlic from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var sessao, rodada, codif, itemp, vaunl, vatol, status, subem, numlic sql.NullString
		err = rows.Scan(&sessao, &rodada, &codif, &itemp, &vaunl, &vatol, &status, &subem, &numlic)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(sessao, rodada, codif, itemp, vaunl, vatol, status, subem, numlic)
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

func Cadpro_final(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec(`DELETE FROM CADPRO_FINAL`)

	insert, err := cnx_dest.Prepare(`INSERT INTO CADPRO_FINAL (NUMLIC, ULT_SESSAO, codif, itemp, VAUNF, vatof, STATUS, subem) VALUES (?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := conexao.Cnx.Query(`SELECT NUMLIC, ULT_SESSAO, codif, itemp, VAUNF, vatof, STATUS, subem FROM CADPRO_FINAL WHERE NUMLIC IN (SELECT NUMLIC FROM CADLIC WHERE ANO <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var NUMLIC, ULT_SESSAO, codif, itemp, VAUNF, vatof, STATUS, subem sql.NullString
		err = rows.Scan(&NUMLIC, &ULT_SESSAO, &codif, &itemp, &VAUNF, &vatof, &STATUS, &subem)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(NUMLIC, ULT_SESSAO, codif, itemp, VAUNF, vatof, STATUS, subem)
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

func Cadpro(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec(`DELETE FROM CADPRO`)

	insert, err := cnx_dest.Prepare(`INSERT INTO CADPRO (CODIF,
		CADPRO,
		QUAN1,
		VAUN1,
		VATO1,
		SUBEM,
		STATUS,
		ITEM,
		NUMORC,
		ITEMORCPED,
		CODCCUSTO,
		FICHA,
		ELEMENTO,
		DESDOBRO,
		NUMLIC,
		ULT_SESSAO,
		ITEMP,
		QTDADT,
		QTDPED,
		VAUNADT,
		VATOADT,
		PERC,
		QTDSOL,
		ID_CADORC,
		VATOPED,
		VATOSOL,
		TPCONTROLE_SALDO,
		MARCA) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := conexao.Cnx.Query(`select CODIF,
											CADPRO,
											QUAN1,
											VAUN1,
											VATO1,
											SUBEM,
											STATUS,
											ITEM,
											NUMORC,
											ITEMORCPED,
											CODCCUSTO,
											FICHA,
											ELEMENTO,
											DESDOBRO,
											NUMLIC,
											ULT_SESSAO,
											ITEMP,
											QTDADT,
											QTDPED,
											VAUNADT,
											VATOADT,
											PERC,
											QTDSOL,
											ID_CADORC,
											VATOPED,
											VATOSOL,
											TPCONTROLE_SALDO,
											MARCA from cadpro where NUMLIC in (select NUMLIC from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx,_ := cnx_dest.Begin()
	for rows.Next() {
		var CODIF, CADPRO, QUAN1, VAUN1, VATO1, SUBEM, STATUS, ITEM, NUMORC, ITEMORCPED, CODCCUSTO, FICHA, ELEMENTO, DESDOBRO, NUMLIC, ULT_SESSAO, ITEMP, QTDADT, QTDPED, VAUNADT, VATOADT, PERC, QTDSOL, ID_CADORC, VATOPED, VATOSOL, TPCONTROLE_SALDO, MARCA sql.NullString
		err = rows.Scan(&CODIF, &CADPRO, &QUAN1, &VAUN1, &VATO1, &SUBEM, &STATUS, &ITEM, &NUMORC, &ITEMORCPED, &CODCCUSTO, &FICHA, &ELEMENTO, &DESDOBRO, &NUMLIC, 
						&ULT_SESSAO, &ITEMP, &QTDADT, &QTDPED, &VAUNADT, &VATOADT, &PERC, &QTDSOL, &ID_CADORC, &VATOPED, &VATOSOL, &TPCONTROLE_SALDO, &MARCA)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert.Exec(CODIF, CADPRO, QUAN1, VAUN1, VATO1, SUBEM, STATUS, ITEM, NUMORC, ITEMORCPED, CODCCUSTO, FICHA, ELEMENTO, DESDOBRO, NUMLIC, ULT_SESSAO, ITEMP, QTDADT, QTDPED, VAUNADT, VATOADT, PERC, QTDSOL, ID_CADORC, VATOPED, VATOSOL, TPCONTROLE_SALDO, MARCA)
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

	cnx_dest.Exec(`insert into cadprolic_detalhe_fic (numlic, item, codigo, qtd, valor, qtdadt, valoradt, codccusto, qtdmed, valormed, tipo) 
					select numlic, item, '0', quan1, vato1, qtdadt, vatoadt, codccusto, quan1, vato1, 'C' from cadpro where numlic in 
					(select numlic from cadlic where registropreco='N' and liberacompra='S') and subem=1;`)
}

func Regpreco(ano int, cnx_dest *sql.DB) {
	cnx_dest.Exec(`DELETE FROM REGPRECODOC`)
	cnx_dest.Exec(`DELETE FROM REGPRECOHIS`)
	cnx_dest.Exec(`DELETE FROM REGPRECO`)

	regprecodoc, err := conexao.Cnx.Query(`select NUMLIC, CODATUALIZACAO, DTPRAZO, ULTIMA from regprecodoc where numlic in (select numlic from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	insert_regprecodoc, err := cnx_dest.Prepare(`insert into REGPRECODOC (NUMLIC, CODATUALIZACAO, DTPRAZO, ULTIMA) VALUES (?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	regpreco, err := conexao.Cnx.Query(`select COD, DTPRAZO, NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, QTDENT, SUBEM, STATUS, ULTIMA from regpreco where numlic in (select numlic from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	insert_regpreco, err := cnx_dest.Prepare(`insert into REGPRECO (COD, DTPRAZO, NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, QTDENT, SUBEM, STATUS, ULTIMA) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	regprecohis, err := conexao.Cnx.Query(`select NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, SUBEM, STATUS, MOTIVO, MARCA, NUMORC, ULTIMA from regprecohis where numlic in (select numlic from cadlic where ano <= ?)`, ano)
	if err != nil {
		fmt.Println(err)
		return
	}

	insert_regprecohis, err := cnx_dest.Prepare(`insert into REGPRECOHIS (NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, SUBEM, STATUS, MOTIVO, MARCA, NUMORC, ULTIMA) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
		return
	}

	tx_regprecodoc,_ := cnx_dest.Begin()
	for regprecodoc.Next() {
		var NUMLIC, CODATUALIZACAO, DTPRAZO, ULTIMA sql.NullString
		err = regprecodoc.Scan(&NUMLIC, &CODATUALIZACAO, &DTPRAZO, &ULTIMA)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert_regprecodoc.Exec(NUMLIC, CODATUALIZACAO, DTPRAZO, ULTIMA)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	err = tx_regprecodoc.Commit()
	if err != nil {
		fmt.Println(err)
		return
	}

	tx_regpreco,_ := cnx_dest.Begin()
	for regpreco.Next() {
		var COD, DTPRAZO, NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, QTDENT, SUBEM, STATUS, ULTIMA sql.NullString
		err = regpreco.Scan(&COD, &DTPRAZO, &NUMLIC, &CODIF, &CADPRO, &CODCCUSTO, &ITEM, &CODATUALIZACAO, &QUAN1, &VAUN1, &VATO1, &QTDENT, &SUBEM, &STATUS, &ULTIMA)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert_regpreco.Exec(COD, DTPRAZO, NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, QTDENT, SUBEM, STATUS, ULTIMA)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	err = tx_regpreco.Commit()
	if err != nil {
		fmt.Println(err)
		return
	}

	tx_regprecohis,_ := cnx_dest.Begin()
	for regprecohis.Next() {
		var NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, SUBEM, STATUS, MOTIVO, MARCA, NUMORC, ULTIMA sql.NullString
		err = regprecohis.Scan(&NUMLIC, &CODIF, &CADPRO, &CODCCUSTO, &ITEM, &CODATUALIZACAO, &QUAN1, &VAUN1, &VATO1, &SUBEM, &STATUS, &MOTIVO, &MARCA, &NUMORC, &ULTIMA)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = insert_regprecohis.Exec(NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, SUBEM, STATUS, MOTIVO, MARCA, NUMORC, ULTIMA)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	err = tx_regprecohis.Commit()
	if err != nil {
		fmt.Println(err)
		return
	}

	cnx_dest.Exec(`insert into cadprolic_detalhe_fic (numlic, item, codigo, qtd, valor, qtdadt, valoradt, codccusto, qtdmed, valormed, tipo)
					select numlic, item, '0', quan1, vato1, quan1, vato1, codccusto, quan1, vato1, 'C' from regpreco where numlic in 
					(select numlic from cadlic where registropreco='S' and liberacompra='S') and subem=1;`)
}