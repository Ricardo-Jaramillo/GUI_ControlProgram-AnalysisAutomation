lis_agg_existentes_bc = [
            ['Mes'],
            ['Region', 'Estado'],
            ['Formato'],
            ['Region', 'Estado', 'Formato', 'Tienda'],
            ['NSE'],
            ['Familia'],
            ['Class'],
            ['Class', 'Subclass'],
            ['Class', 'Subclass', 'ProdType'],
            ['Class', 'Subclass', 'ProdType', 'Producto']
        ]

lis_agg = [
            'Mes',

            'Region',
            'Estado',
            'Formato',
            'Tienda',

            'Familia',
            'NSE',

            'Class',
            'Subclass',
            'ProdType',
            'Producto'
        ]

dict_map = {
            'region': {
                '10 REGIÓN METROPOLITANA': 'Metropolitana',
                '20 REGIÓN ORIENTE': 'Oriente',
                '30 REGIÓN SUR': 'Sur',
                '40 REGIÓN PACÍFICO': 'Pacifico',
                '45 REGIÓN BAJÍO': 'Bajio',
                '48 REGIÓN NORESTE': 'Noreste',
                '50 REGIÓN COMERCIO ELECTRÓNICO': 'Comercio Electronico',
                '60 REGIÓN VENTAS CENTRAL': 'Ventas Central',
                '70 REGIÓN BODEGAS': 'Bodegas'
            },
            'state': {
                'Aguascalientes': 'Aguascalientes',
                'Baja California': 'Baja California',
                'Baja California Sur': 'Baja California Sur',
                'Campeche': 'Campeche',
                'Chiapas': 'Chiapas',
                'Ciudad de México': 'Ciudad de Mexico',
                'Distrito Federal': 'Ciudad de Mexico',
                'Durango': 'Durango',
                'Estado de México': 'Estado de Mexico',
                'Guanajuato': 'Guanajuato',
                'Guerrero': 'Guerrero',
                'Hidalgo': 'Hidalgo',
                'Jalisco': 'Jalisco',
                'Michoacán': 'Michoacan',
                'Morelos': 'Morelos',
                'Nayarit': 'Nayarit',
                'Nuevo León': 'Nuevo Leon',
                'Oaxaca': 'Oaxaca',
                'Puebla': 'Puebla',
                'Querétaro': 'Queretaro',
                'Quintana Roo': 'Quintana Roo',
                'San Luis Potosí': 'San Luis Potosi',
                'Sinaloa': 'Sinaloa',
                'Tabasco': 'Tabasco',
                'Tamaulipas': 'Tamaulipas',
                'Tlaxcala': 'Tlaxcala',
                'Veracruz': 'Veracruz',
                'Yucatán': 'Yucatan',
                'Zacatecas': 'Zacatecas'
            },
            'formato_tienda': {
                '01 SELECTO': 'Selecto',
                '02 AB': 'AB',
                '03 CD': 'CD',
                '04 WEB': 'Web',
                '05 SUPERCITO': 'Supercito',
            },
            'tipo_familia': {
                'FAMILIA_BEBES': '1 Bebes',
                'FAMILIA_NINOS': '2 Ninos',
                'FAMILIA_JOVENES': '3 Jovenes',
                'JOVEN/VIVO_SOLO': '4 Joven/Vive Solo',
                'PAREJA_MADURA': '5 Pareja Madura',
                'NO SEGMENTADO': '6 No Segmentado',
            },
            'nse': {
                'Alto': '1 Alto',
                'Bajo': '3 Bajo',
                'Medio': '2 Medio',
                'NO SEGMENTADO': '4 No Segmentado',
            }
        }
        