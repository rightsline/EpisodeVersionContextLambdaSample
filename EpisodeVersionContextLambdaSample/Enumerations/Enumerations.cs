﻿using System;
using System.ComponentModel;
using System.Reflection;

namespace EpisodeVersionContextLambdaSample
{
    public enum CharTypeID
    {
        [Description("relationship")]
        Relationship = 0,
        [Description("catalog-item")]
        CatalogItem = 1
    }

    public static class Enumerations
    {
        public static string GetEnumDescription(Enum value)
        {
            FieldInfo fi = value.GetType().GetField(value.ToString());

            DescriptionAttribute[] attributes =
                (DescriptionAttribute[])fi.GetCustomAttributes(
                typeof(DescriptionAttribute),
                false);

            if (attributes != null &&
                attributes.Length > 0)
                return attributes[0].Description;
            else
                return value.ToString();
        }
    }
}
