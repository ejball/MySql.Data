﻿using System;

namespace MySql.Data.MySqlClient
{
	[Flags]
	internal enum StatementPreparerOptions
	{
		None = 0,
		AllowUserVariables = 1,
	}
}
