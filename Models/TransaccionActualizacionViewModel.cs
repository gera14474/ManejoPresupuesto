﻿using ManejoPresupuesto.Models;

namespace ManejoPresupuesto.Models
{
    public class TransaccionActualizacionViewModel: TransaccionCreacionViewModel
    {
        public int CuentaAnteriorId { get; set; }
        public decimal MontoAnterior { get; set; }
        public string urlRetorno { get; set; }
    }
}
