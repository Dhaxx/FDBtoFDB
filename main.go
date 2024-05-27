package main

import (
	"FDBtoFDB/conexao"
	"FDBtoFDB/compras"
)

func main() {
	for i := 2023; i >= 2020; i-- {
		var cnx_dest, _ = conexao.ConexaoFirebird(i)
		///////////////////
		// compras.Cadunimedida(cnx_dest)
		// compras.GrupoSubgrupo(cnx_dest)
		// compras.Cadest(cnx_dest)
		// compras.Destino(cnx_dest)
		// compras.CentroCusto(cnx_dest)

		// compras.Cadorc(i, cnx_dest)
		// compras.Icadorc(i, cnx_dest)
		// compras.Icadorc_cot(i, cnx_dest)
		// compras.Fcadorc(i, cnx_dest)
		// compras.Vcadorc(i, cnx_dest)
		compras.LimpaLics(cnx_dest)
		compras.Cadlic(i, cnx_dest)
		compras.Cadprolic(i, cnx_dest)
		compras.Cadprolic_detalhe(i, cnx_dest)
		compras.ProlicProlics(i, cnx_dest)
		compras.Cadpro_status(i, cnx_dest)
		compras.Cadpro_proposta(i, cnx_dest)
		compras.Cadpro_lance(i, cnx_dest)
		compras.Cadpro_final(i, cnx_dest)
		compras.Cadpro(i, cnx_dest)
		compras.Regpreco(i, cnx_dest)
		compras.LimpaPedidos(cnx_dest)
		compras.Cadped(i, cnx_dest)
		compras.Icadped(i, cnx_dest)
		///////////////////
		cnx_dest.Close()
	}
}