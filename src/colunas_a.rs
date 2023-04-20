use csv::StringRecord;
use serde::{Serialize, Deserialize};
use serde_aux::prelude::serde_introspect;
use chrono::{DateTime, Utc};
use data_manipulation::ExtraProperties;

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Clone)]
pub struct ColunasA {
    #[serde(rename = "Linhas")]
    pub linhas: Option<usize>,
    #[serde(rename = "Arquivo da EFD Contribuições")] // não poder conter vírgulas
    pub arquivo_efd: String,
    #[serde(rename = "Nº da Linha da EFD", deserialize_with = "csv::invalid_option")]
    pub num_linha_efd: Option<usize>,
    #[serde(rename = "CNPJ dos Estabelecimentos do Contribuinte")]
    pub estabelecimento_cnpj: String,
    #[serde(rename = "Nome do Contribuinte")]
    pub estabelecimento_nome: String,
    #[serde(rename = "Ano do Período de Apuração", deserialize_with = "csv::invalid_option")]
    pub ano: Option<u32>,
    #[serde(rename = "Trimestre do Período de Apuração", deserialize_with = "csv::invalid_option")]
    pub trimestre: Option<u32>,
    #[serde(rename = "Mês do Período de Apuração", deserialize_with = "csv::invalid_option")]
    pub mes: Option<u32>,
    #[serde(rename = "Tipo de Operação")]
    pub tipo_de_operacao: String,
    #[serde(rename = "Indicador de Origem")]
    pub indicador_de_origem: String,
    #[serde(rename = "Código do Tipo de Crédito", deserialize_with = "csv::invalid_option")]
    pub cod_cred: Option<u16>,
    #[serde(rename = "Tipo de Crédito", deserialize_with = "csv::invalid_option")]
    pub tipo_de_credito: Option<u16>,
    #[serde(rename = "Registro")]
    pub registro: String,
    #[serde(rename = "Código de Situação Tributária (CST)", deserialize_with = "csv::invalid_option")]
    pub cst: Option<u16>,
    #[serde(rename = "Código Fiscal de Operações e Prestações (CFOP)", deserialize_with = "csv::invalid_option")]
    pub cfop: Option<u16>,
    #[serde(rename = "Natureza da Base de Cálculo dos Créditos", deserialize_with = "csv::invalid_option")]
    pub natureza_bc: Option<u16>,
    #[serde(rename = "CNPJ do Participante")]
    pub particante_cnpj: String,
    #[serde(rename = "CPF do Participante")]
    pub particante_cpf: String,
    #[serde(rename = "Nome do Participante")]
    pub particante_nome: String,
    #[serde(rename = "Nº do Documento Fiscal", deserialize_with = "csv::invalid_option")]
    pub num_doc: Option<usize>,
    #[serde(rename = "Chave do Documento")]
    pub chave_doc: String,
    #[serde(rename = "Modelo do Documento Fiscal")]
    pub modelo_doc_fiscal: String,
    #[serde(rename = "Nº do Item do Documento Fiscal", deserialize_with = "csv::invalid_option")]
    pub num_item: Option<usize>,
    #[serde(rename = "Tipo do Item")]
    pub tipo_item: String,
    #[serde(rename = "Descrição do Item")]
    pub descr_item: String,
    #[serde(rename = "Código NCM")]
    pub cod_ncm: String,
    #[serde(rename = "Natureza da Operação/Prestação")]
    pub nat_operacao: String,
    #[serde(rename = "Informação Complementar do Documento Fiscal")]
    pub complementar: String,
    #[serde(rename = "Escrituração Contábil: Nome da Conta")]
    pub nome_da_conta: String,
    #[serde(rename = "Data da Emissão do Documento Fiscal", deserialize_with = "csv::invalid_option")]
    pub data_emissao: Option<DateTime<Utc>>,
    #[serde(rename = "Data da Entrada / Aquisição / Execução ou da Saída / Prestação / Conclusão", deserialize_with = "csv::invalid_option")]
    pub data_lancamento: Option<DateTime<Utc>>,
    #[serde(rename = "Valor Total do Item", deserialize_with = "csv::invalid_option")]
    pub valor_item: Option<f64>,
    #[serde(rename = "Valor da Base de Cálculo das Contribuições", deserialize_with = "csv::invalid_option")]
    pub valor_bc: Option<f64>,
    #[serde(rename = "Alíquota de PIS/PASEP (em percentual)", deserialize_with = "csv::invalid_option")]
    pub aliq_pis: Option<f64>,
    #[serde(rename = "Alíquota de COFINS (em percentual)", deserialize_with = "csv::invalid_option")]
    pub aliq_cofins: Option<f64>,
    #[serde(rename = "Valor de PIS/PASEP", deserialize_with = "csv::invalid_option")]
    pub valor_pis: Option<f64>,
    #[serde(rename = "Valor de COFINS", deserialize_with = "csv::invalid_option")]
    pub valor_cofins: Option<f64>,
    #[serde(rename = "Valor de ISS", deserialize_with = "csv::invalid_option")]
    pub valor_iss: Option<f64>,
    #[serde(rename = "Valor da Base de Cálculo de ICMS", deserialize_with = "csv::invalid_option")]
    pub valor_bc_icms: Option<f64>,
    #[serde(rename = "Alíquota de ICMS (em percentual)", deserialize_with = "csv::invalid_option")]
    pub aliq_icms: Option<f64>,
    #[serde(rename = "Valor de ICMS", deserialize_with = "csv::invalid_option")]
    pub valor_icms: Option<f64>,
}

// https://stackoverflow.com/questions/72737381/get-structure-header-names-with-serde-serialize-preserving-order/72745649#72745649

impl ColunasA {
    #[allow(dead_code)]
    pub fn get_headers() -> StringRecord {
        // use serde_aux::prelude::serde_introspect;
        let colunas_vec = serde_introspect::<ColunasA>();
        StringRecord::from(colunas_vec)
    }

    #[allow(dead_code)]
    pub fn get_number_of_fields() -> usize {
        // use serde_aux::prelude::serde_introspect;
        let colunas_vec = serde_introspect::<ColunasA>();
        let number_of_fields = colunas_vec.len();
        println!("number_of_fields: {number_of_fields}");
        number_of_fields
    }

    #[allow(dead_code)]
    pub fn credito_valido(&self) -> bool {
        self.cst >= Some(50) && self.cst <= Some(66) && self.natureza_bc.is_some()
    }

    #[allow(dead_code)]
    pub fn format(&mut self) {

        // https://stackoverflow.com/questions/30154541/how-do-i-concatenate-strings
        // 14 digits: exemplo CNPJ: 01234567000890 --> 01.234.567/0008-90
        // 11 digits: exemplo CPF: 12345678901     --> 123.456.789-01
        //  8 digits: exemplo NCM: 01234567        --> 0123.45.67

        let cnpjs = vec![
            &mut self.estabelecimento_cnpj,
            &mut self.particante_cnpj,
        ];

        for cnpj in cnpjs {
            if cnpj.contains_num_digit(14) {
                *cnpj = [&cnpj[0..2], ".", &cnpj[2..5], ".", &cnpj[5..8], "/", &cnpj[8..12], "-", &cnpj[12..]].concat();
            }
        }

        let cpf = &mut self.particante_cpf;

        if cpf.contains_num_digit(11) {
            *cpf = [&cpf[0..3], ".", &cpf[3..6], ".", &cpf[6..9], "-", &cpf[9..]].concat();
        }

        let ncm = &mut self.cod_ncm;

        if ncm.contains_num_digit(8) {
            *ncm = [&ncm[0..4], ".", &ncm[4..6], ".", &ncm[6..]].concat();
        }
    }

    #[allow(dead_code)]
    pub fn line_update(&mut self) {
        self.linhas = self.linhas.map(|number| number + 1)
    }
}

#[cfg(test)]
mod tests {
    // cargo test -- --help
    // cargo test -- --nocapture
    // cargo test -- --show-output
    use super::*;

    #[test]
    fn show_headers() {
        ColunasA::get_number_of_fields();
        let headers = ColunasA::get_headers();
        println!("ColunasA headers: {headers:#?}");
        assert_eq!(&headers[0], "Linhas");
        assert_eq!(&headers[1], "Arquivo da EFD Contribuições");
    }

    #[test]
    fn definir_colunas() {
        let mut colunas = ColunasA {..Default::default()};
        colunas.cst = Some(56);
        colunas.estabelecimento_cnpj = "01234567000890".to_string();
        colunas.particante_cnpj = "12345678000912".to_string();
        colunas.particante_cpf = "12345678901".to_string();
        colunas.cod_ncm = "01234567".to_string();
        println!("ColunasA: {colunas:#?}");

        colunas.line_update();
        colunas.format();
        println!("ColunasA: {colunas:#?}");
        assert_eq!(colunas.cst, Some(56));
        assert_eq!(colunas.estabelecimento_cnpj, "01.234.567/0008-90");
        assert_eq!(colunas.particante_cnpj, "12.345.678/0009-12");
        assert_eq!(colunas.particante_cpf, "123.456.789-01");
        assert_eq!(colunas.cod_ncm, "0123.45.67");
    }
}
