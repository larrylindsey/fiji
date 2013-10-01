package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.network.translation.Bottle;
import edu.utexas.clm.archipelago.network.translation.Bottler;
import ini.trakem2.display.Patch;

/**
 *
 */
public class PatchBottler implements Bottler<Patch>
{
    public boolean accepts(Object o)
    {
        return o instanceof Patch;
    }

    public Bottle<Patch> bottle(Object o)
    {
        return new PatchBottle((Patch)o);
    }

    public boolean transfer()
    {
        return false;
    }
}
