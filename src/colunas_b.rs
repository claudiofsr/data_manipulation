use csv::StringRecord;
use serde::{Serialize, Deserialize};
use serde_aux::prelude::serde_introspect;
use chrono::{DateTime, Utc};

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Clone)]
pub struct ColunasB {
    #[serde(rename = "CNPJ do Contribuinte : NF Item (Todos)", deserialize_with = "csv::invalid_option")] // não poder conter vírgulas
    pub cnpj_do_contribuinte: Option<String>,
    #[serde(rename = "Nome do Contribuinte : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub nome_do_contribuinte: Option<String>,
    #[serde(rename = "Entrada/Saída : NF (Todos)", deserialize_with = "csv::invalid_option")]
    pub entrada_saida: Option<String>,
    #[serde(rename = "CPF/CNPJ do Participante : NF (Todos)", deserialize_with = "csv::invalid_option")]
    pub cpf_cnpj_do_participante: Option<String>,
    #[serde(rename = "Nome do Participante : NF (Todos)", deserialize_with = "csv::invalid_option")]
    pub nome_do_participante: Option<String>,
    #[serde(rename = "CRT : NF (Todos)", deserialize_with = "csv::invalid_option")]
    pub crt: Option<String>,
    #[serde(rename = "Observações : NF (Todos)", deserialize_with = "csv::invalid_option")]
    pub observacoes: Option<String>,
    #[serde(rename = "CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", deserialize_with = "csv::invalid_option")]
    pub cte_remetente_das_mercadorias_transportadas_cnpj_cpf_de_conhecimento_valores: Option<String>,
    #[serde(rename = "CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento : ConhecimentoInformacaoNFe", deserialize_with = "csv::invalid_option")]
    pub cte_remetente_das_mercadorias_transportadas_cnpj_cpf_de_conhecimento_informacao: Option<String>,
    #[serde(rename = "CTe - Remetente das mercadorias transportadas: Nome de Conhecimento : ConhecimentoInformacaoNFe", deserialize_with = "csv::invalid_option")]
    pub cte_remetente_das_mercadorias_transportadas_nome_de_conhecimento_informacao: Option<String>,
    #[serde(rename = "CTe - Remetente das mercadorias transportadas: Município de Conhecimento : ConhecimentoInformacaoNFe", deserialize_with = "csv::invalid_option")]
    pub cte_remetente_das_mercadorias_transportadas_municipio: Option<String>,
    #[serde(rename = "Descrição CTe - Indicador do 'papel' do tomador do serviço de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", deserialize_with = "csv::invalid_option")]
    pub descricao_cte_indicador_do_papel_do_tomador_do_servico_valores: Option<String>,
    #[serde(rename = "Descrição CTe - Indicador do 'papel' do tomador do serviço de Conhecimento : ConhecimentoInformacaoNFe", deserialize_with = "csv::invalid_option")]
    pub descricao_cte_indicador_do_papel_do_tomador_do_servico_informacao: Option<String>,
    #[serde(rename = "CTe - Outro tipo de Tomador: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", deserialize_with = "csv::invalid_option")]
    pub cte_outro_tipo_de_tomador_cnpj_cpf_valores: Option<String>,
    #[serde(rename = "CTe - Outro tipo de Tomador: CNPJ/CPF de Conhecimento : ConhecimentoInformacaoNFe", deserialize_with = "csv::invalid_option")]
    pub cte_outro_tipo_de_tomador_cnpj_cpf_informacao: Option<String>,
    #[serde(rename = "CTe - UF do início da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", deserialize_with = "csv::invalid_option")]
    pub cte_uf_do_inicio_da_prestacao_de_conhecimento: Option<String>,
    #[serde(rename = "CTe - Nome do Município do início da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", deserialize_with = "csv::invalid_option")]
    pub cte_municipio_do_inicio_da_prestacao_de_conhecimento: Option<String>,
    #[serde(rename = "CTe - UF do término da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", deserialize_with = "csv::invalid_option")]
    pub cte_uf_do_termino_da_prestacao_de_conhecimento: Option<String>,
    #[serde(rename = "CTe - Nome do Município do término da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", deserialize_with = "csv::invalid_option")]
    pub cte_municipio_do_termino_da_prestacao_de_conhecimento: Option<String>,
    #[serde(rename = "CTe - Informações do Destinatário do CT-e: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", deserialize_with = "csv::invalid_option")]
    pub cte_informacoes_do_destinatario_cnpj_cpf: Option<String>,
    #[serde(rename = "CTe - Informações do Destinatário do CT-e: Nome de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", deserialize_with = "csv::invalid_option")]
    pub cte_informacoes_do_destinatario_nome: Option<String>,
    #[serde(rename = "CTe - Local de Entrega constante na Nota Fiscal: Nome de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", deserialize_with = "csv::invalid_option")]
    pub cte_local_de_entrega: Option<String>,
    #[serde(rename = "Descrição da Natureza da Operação : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub descricao_da_natureza_da_operacao: Option<String>,
    #[serde(rename = "Cancelada : NF (Todos)", deserialize_with = "csv::invalid_option")]
    pub cancelada: Option<String>,
    #[serde(rename = "Registro de Origem do Item : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub registro_de_origem_do_item: Option<String>,
    #[serde(rename = "Natureza da Base de Cálculo do Crédito Descrição : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub natureza_da_base_de_calculo_do_credito_descricao: Option<String>,
    #[serde(rename = "Modelo - Descrição : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub modelo_descricao: Option<String>,
    #[serde(rename = "Número da Nota : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub numero_da_nota: Option<usize>,
    #[serde(rename = "Chave da Nota Fiscal Eletrônica : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub chave_doc: Option<String>,
    #[serde(rename = "Inf. NFe - Chave de acesso da NF-e : ConhecimentoInformacaoNFe", deserialize_with = "csv::invalid_option")]
    pub inf_nfe_chave_de_acesso: Option<String>,
    #[serde(rename = "CTe - Observações Gerais de Conhecimento : ConhecimentoInformacaoNFe", deserialize_with = "csv::invalid_option")]
    pub cte_observacoes_gerais: Option<String>,
    #[serde(rename = "Dia da Emissão : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub dia_da_emissao: Option<DateTime<Utc>>,
    #[serde(rename = "Número da DI : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub numero_da_di: Option<String>,
    #[serde(rename = "Número do Item : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub numero_do_item: Option<usize>,
    #[serde(rename = "Código CFOP : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub codigo_cfop: Option<usize>,
    #[serde(rename = "Descrição CFOP : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub descricao_cfop: Option<String>,
    #[serde(rename = "Descrição da Mercadoria/Serviço : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub descricao_da_mercadoria: Option<String>,
    #[serde(rename = "Código NCM : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub codigo_ncm: Option<usize>,
    #[serde(rename = "Descrição NCM : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub descricao_ncm: Option<String>,
    #[serde(rename = "COFINS: Alíquota ad valorem - Atributo : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub cofins_aliquota_ad_valorem: Option<usize>,
    #[serde(rename = "PIS: Alíquota ad valorem - Atributo : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub pis_aliquota_ad_valorem: Option<usize>,
    #[serde(rename = "CST COFINS Descrição : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub cst_cofins_descricao: Option<String>,
    #[serde(rename = "CST PIS Descrição : NF Item (Todos)", deserialize_with = "csv::invalid_option")]
    pub cst_pis_descricao: Option<String>,
    #[serde(rename = "Valor Total : NF (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub valor_total: Option<f64>,
    #[serde(rename = "Valor da Nota Proporcional : NF Item (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub valor_item: Option<f64>,
    #[serde(rename = "Valor dos Descontos : NF Item (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub valor_dos_descontos: Option<f64>,
    #[serde(rename = "Valor Seguro : NF (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub valor_do_seguro: Option<f64>,
    #[serde(rename = "COFINS: Valor do Tributo : NF Item (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub cofins_valor_do_tributo: Option<f64>,
    #[serde(rename = "PIS: Valor do Tributo : NF Item (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub pis_valor_do_tributo: Option<f64>,
    #[serde(rename = "IPI: Valor do Tributo : NF Item (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub ipi_valor_do_tributo: Option<f64>,
    #[serde(rename = "ISS: Base de Cálculo : NF Item (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub iss_base_de_calculo: Option<f64>,
    #[serde(rename = "ISS: Valor do Tributo : NF Item (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub iss_valor_do_tributo: Option<f64>,
    #[serde(rename = "ICMS: Alíquota : NF Item (Todos) NOISE OR", deserialize_with = "csv::invalid_option")]
    pub icms_aliquota: Option<f64>,
    #[serde(rename = "ICMS: Base de Cálculo : NF Item (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub icms_base_de_calculo: Option<f64>,
    #[serde(rename = "ICMS: Valor do Tributo : NF Item (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub icms_valor_do_tributo: Option<f64>,
    #[serde(rename = "ICMS por Substituição: Valor do Tributo : NF Item (Todos) SOMA", deserialize_with = "csv::invalid_option")]
    pub icms_por_substituicao: Option<f64>,
}

// https://stackoverflow.com/questions/72737381/get-structure-header-names-with-serde-serialize-preserving-order/72745649#72745649

impl ColunasB {
    #[allow(dead_code)]
    pub fn get_headers() -> StringRecord {
        // use serde_aux::prelude::serde_introspect;
        let colunas_vec = serde_introspect::<ColunasB>();
        StringRecord::from(colunas_vec)
    }

    #[allow(dead_code)]
    pub fn get_number_of_fields() -> usize {
        // use serde_aux::prelude::serde_introspect;
        let colunas_vec = serde_introspect::<ColunasB>();
        let number_of_fields = colunas_vec.len();
        println!("number_of_fields: {number_of_fields}");
        number_of_fields
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
        ColunasB::get_number_of_fields();
        let headers = ColunasB::get_headers();
        println!("ColunasB headers: {headers:#?}");
        assert_eq!(&headers[0], "Linhas");
        assert_eq!(&headers[1], "Arquivo da EFD Contribuições");
    }
}
